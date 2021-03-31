package com.fadada.log.stream;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class LogStream {

    static final Logger logger = LoggerFactory.getLogger(LogStream.class);

    public static final String[] WORDS = new String[]{
            "{\"@timestamp\":1616653682.333932,\"log\":\"2021-03-25 16:40:51.544 | /contract/rec-sign | b6a4d3305f5f446da1371d62983e9a9d | ERROR | paas-service-contract | contract-dp-56bf69777d-5rz9d | [http-nio-8890-exec-41] | c.f.c.c.s.i.c.ContractRecSignServiceImpl | [TxId] : api-signtask-service^1615885704473^621129 # [SpanId] : -4053522222648396863 | 远程签章服务失败:签章不存在\"}",
            "{\"@timestamp\":1616653682.333932,\"log\":\"2021-03-25 16:40:51.544 | /contract/rec-sign | b6a4d3305f5f446da1371d62983e9a9d | ERROR | paas-service-contract | contract-dp-56bf69777d-5rz9d | [http-nio-8890-exec-41] | c.f.c.c.s.i.c.ContractRecSignServiceImpl | [TxId] : api-signtask-service^1615885704473^621129 # [SpanId] : -4053522222648396863 | 远程签章服务失败:签章不存在\"}",
            "{\"@timestamp\":1616653682.333932,\"log\":\"2021-03-25 16:40:51.544 | /contract/rec-sign | b6a4d3305f5f446da1371d62983e9a9d | INFO | paas-service-contract | contract-dp-56bf69777d-5rz9d | [http-nio-8890-exec-41] | c.f.c.c.s.i.c.ContractRecSignServiceImpl | [TxId] : api-signtask-service^1615885704473^621129 # [SpanId] : -4053522222648396863 | 远程签章服务失败:签章不存在\"}",
            "{\"@timestamp\":1616653682.333932,\"log\":\"2021-03-25 16:40:51.544 | /contract/rec-sign | b6a4d3305f5f446da1371d62983e9a9d | ERROR | paas-service-contract | contract-dp-56bf69777d-5rz9d | [http-nio-8890-exec-41] | c.f.c.c.s.i.c.ContractRecSignServiceImpl | [TxId] : api-signtask-service^1615885704473^621129 # [SpanId] : -4053522222648396863 | 远程签章服务失败:签章不存在\"}",
            "{\"@timestamp\":1616653682.333932,\"log\":\"2021-03-25 16:40:51.544 | /contract/rec-sign | b6a4d3305f5f446da1371d62983e9a9d | INFO | paas-service-account | contract-dp-56bf69777d-5rz9d | [http-nio-8890-exec-41] | c.f.c.c.s.i.c.ContractRecSignServiceImpl | [TxId] : api-signtask-service^1615885704473^621129 # [SpanId] : -4053522222648396863 | 远程签章服务失败:签章不存在\"}",
            "{\"@timestamp\":1616653682.333932,\"log\":\"2021-03-25 16:40:51.544 | /contract/rec-sign | b6a4d3305f5f446da1371d62983e9a9d | ERROR | paas-service-account | contract-dp-56bf69777d-5rz9d | [http-nio-8890-exec-41] | c.f.c.c.s.i.c.ContractRecSignServiceImpl | [TxId] : api-signtask-service^1615885704473^621129 # [SpanId] : -4053522222648396863 | 远程签章服务失败:签章不存在\"}",
    };

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


       // DataStream<String> text = env.fromElements(WORDS);

        SingleOutputStreamOperator<LogAggregation> map = streamOperator.process(new ProcessFunction<String, LogAggregation>() {
            @Override
            public void processElement(String value, Context ctx, Collector<LogAggregation> out) throws Exception {
                JSONObject json = JSONObject.parseObject(value);
                String log = json.getString("log");
                Long timestamp = json.getLongValue("@timestamp");
                String[] split = log.split("\\|");
                if(split.length == 10){
                    LogMetadata data = new LogMetadata();
                    data.setTimestamp(split[0].trim());
                    data.setPath(split[1].trim());
                    data.setRequestId(split[2].trim());
                    data.setLogLevel(split[3].trim());
                    data.setAppName(split[4].trim());
                    data.setHostName(split[5].trim());
                    data.setThreadName(split[6].trim());
                    data.setClassName(split[7].trim());
                    data.setTxId(split[8].trim());
                    data.setLog(split[9].trim());
                    LogAggregation aggregation = new LogAggregation();
                    aggregation.setAppName(data.getAppName());
                    aggregation.setLog(data);
                    aggregation.setLogStr(value);
                    aggregation.setTimestamp(timestamp);
                    aggregation.setCount(1);
                    out.collect(aggregation);
                }
            }
        });
        System.out.println("=========================filter===================================");

        DataStreamSink<LogWindowResult> streamSink = map.filter((value) -> {
            if(value.getLog().getLogLevel().equals("ERROR") || value.getLog().getLog().contains("error")){
                return true;
            }
            return false;
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<LogAggregation>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((record, ts) -> {
                    logger.info("事件事件 timestamp = " + record.getTimestamp() + " ts = " + ts);
                    return record.getTimestamp();
                }))
                .setParallelism(1)
                .keyBy((value) -> (String)value.getAppName())
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .trigger(new MyTrigger())
                .apply(new WindowFunction<LogAggregation, LogWindowResult, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<LogAggregation> iterable, Collector<LogWindowResult> out) throws Exception {
                        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        logger.info("进入窗口计算, start = {}, end ={} ", dateFormatter.format(window.getStart()), dateFormatter.format(window.getEnd()));
                        List<String> logs = Lists.newArrayList();
                        Iterator<LogAggregation> iterator = iterable.iterator();
                        while(iterator.hasNext()){
                            LogAggregation agg = iterator.next();
                            logs.add(agg.getLogStr());
                        }
                        logger.info("窗口数据:   size = " + logs.size() + JSONObject.toJSONString(logs));
                        if(logs.size() >= 3){
                            LogWindowResult result = new LogWindowResult();
                            result.setAppName(s);
                            result.setLogs(logs);
                            out.collect(result);
                        }
                    }
                }).addSink(new LogSinkToMySQL());
        env.execute("logstream");
    }

    public static class MyTrigger extends Trigger<LogAggregation, TimeWindow> {

        @Override
        public TriggerResult onElement(LogAggregation element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            logger.info("进入onElement：" + "window.maxTimestamp = " + window.maxTimestamp() + ", ctx.getCurrentWatermark() = "
                    + ctx.getCurrentWatermark());
            if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
                return TriggerResult.FIRE;
            } else {
                ctx.registerEventTimeTimer(window.maxTimestamp());
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
            logger.info("进入onEventTime：" + "window.maxTimestamp = " + window.maxTimestamp() + ", time = "
                    + time);
            return time == window.maxTimestamp() ?
                    TriggerResult.FIRE :
                    TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.deleteEventTimeTimer(window.maxTimestamp());
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(TimeWindow window,
                            OnMergeContext ctx) {
            // only register a timer if the watermark is not yet past the end of the merged window
            // this is in line with the logic in onElement(). If the watermark is past the end of
            // the window onElement() will fire and setting a timer here would fire the window twice.
            long windowMaxTimestamp = window.maxTimestamp();
            if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
                ctx.registerEventTimeTimer(windowMaxTimestamp);
            }
        }

        @Override
        public String toString() {
            return "EventTimeTrigger()";
        }
    }
}
