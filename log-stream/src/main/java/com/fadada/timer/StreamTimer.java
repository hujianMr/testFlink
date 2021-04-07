package com.fadada.timer;

import com.alibaba.fastjson.JSONObject;
import com.fadada.log.NonSink;
import com.fadada.log.stream.LogAggregation;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * time 5s内三条错误日志就报警
 */
public class StreamTimer {

    static final Logger logger = LoggerFactory.getLogger(StreamTimer.class);

    static String groupId = "container-log-uat";
    static String topic = "test";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //  env.getConfig().setAutoWatermarkInterval(0);
        env.enableCheckpointing(60000);
        env.setStateBackend(new RocksDBStateBackend("file:///D://flink/rocksdbpath", true));
        env.setParallelism(1);

        Properties props = new Properties();
        props.put("bootstrap.servers", "172.19.64.26:9092");
        //  props.put("zookeeper.connect", "192.168.66.140:2181");
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "true");
        DataStream<String> streamOperator = env.addSource(new FlinkKafkaConsumer<>(
                topic,   //这个 kafka topic 保持一致
                new SimpleStringSchema(),
                props));
        SingleOutputStreamOperator<Tuple4<Long, String, String, String>> stream =
                streamOperator.process(new ProcessFunction<String, Tuple4<Long, String, String, String>>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<Tuple4<Long, String, String, String>> out) throws Exception {
                        JSONObject json = JSONObject.parseObject(value);
                        String log = json.getString("log");
                        Long timestamp = json.getLongValue("@timestamp");
                        String[] split = log.split("\\|");
                        if(split.length == 10){
                            Tuple4<Long, String, String, String> t4 = new Tuple4<>(
                                    timestamp, split[4].trim(), split[3].trim(), log);
                            out.collect(t4);
                        }
                    }
                }).filter(value -> {
                    if(value.f2.equals("ERROR") || value.f3.contains("error")){
                        return true;
                    }
                    return false;
                }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple4<Long, String, String, String>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((tuple4, ts) -> {
                            logger.info("事件时间 timestamp = " + tuple4.f0 + " ts = " + ts);
                            return tuple4.f0;
                        }))
                .keyBy(t -> t.f1)
                .process(new KeyedProcessFunction<String, Tuple4<Long, String, String, String>, Tuple4<Long, String, String, String>>() {

                    private ListState<Tuple4<Long, String, String, String>> tupleListState;
                    private ValueState<Boolean> warned;

                    @Override
                    public void open(Configuration parameters) {
                        tupleListState = getRuntimeContext().getListState(
                                new ListStateDescriptor<>("tupleListState",
                                        TypeInformation.of(new TypeHint<Tuple4<Long, String, String, String>>() {
                                })));
                        warned = getRuntimeContext().getState(new
                                ValueStateDescriptor<>("warned", Boolean.class));
                    }

                    @Override
                    public void processElement(Tuple4<Long, String, String, String> value,
                                               Context ctx, Collector<Tuple4<Long, String, String, String>> out) throws Exception {
                        logger.info("timestamp = {}", ctx.timestamp());
                        tupleListState.add(value);
                        if(warned.value() == null){
                            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 15000L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx,
                                        Collector<Tuple4<Long, String, String, String>> out) throws Exception {
                        logger.info("定时器执行, timestamp = {}, ctx-timestamp = {}", timestamp, ctx.timestamp());
                        List<Tuple4<Long, String, String, String>> result = new ArrayList<>();
                        for (Tuple4<Long, String, String, String> t : tupleListState.get()) {
                            result.add(t);
                        }
                        tupleListState.clear();
                        warned.clear();
                        logger.info("onTimer size = {}", result.size());
                        if(result.size() > 3){
                            result.forEach(t -> out.collect(t));
                            //out.collect();
                        }
                    }
                });
        stream.addSink(new NonSink());
       // stream.print();

        env.execute("timer stream");
    }
}
