package test.flink.model;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.Properties;

public class Main3 {
    private static String topic = "pikaqiu";
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.66.140:9092");
        props.put("zookeeper.connect", "192.168.66.140:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        SingleOutputStreamOperator<Config> streamOperator = env.addSource(new FlinkKafkaConsumer011<>(
                topic,   //这个 kafka topic 保持一致
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(string -> JSON.parseObject(string, Config.class));

        //实现map方法计算
        SingleOutputStreamOperator<Config> map =
                streamOperator.map(new MapFunction<Config, Config>() {
                    @Override
                    public Config map(Config value) throws Exception {
                        Config config = new Config();
                        config.setLabel(value.getLabel());
                        config.setRemark(value.getRemark());
                        config.setValue(value.getValue());
                        config.setVoiceValue(
                                String.valueOf(Integer.parseInt(value.getVoiceValue())* 100));
                        return config;
                    }
                });
     //   map.print();


        //FlatMap 采用一条记录并输出零个，一个或多个记录。   这里将voiceValue为偶数得时候聚合
        SingleOutputStreamOperator<Config> flatMap =
                streamOperator.flatMap(new FlatMapFunction<Config, Config>() {
                    @Override
                    public void flatMap(Config config, Collector<Config> collector) throws Exception {
                        int voiceValue = Integer.parseInt(config.getVoiceValue());
                        if (voiceValue % 2 == 0) {
                            collector.collect(config);
                        }
                    }
                });
    //    flatMap.print();

        //过滤
        SingleOutputStreamOperator<Config> filter = streamOperator.filter(
                new FilterFunction<Config>() {
                    @Override
                    public boolean filter(Config config) throws Exception {
                        int voiceValue = Integer.parseInt(config.getVoiceValue());
                        if(voiceValue > 6){
                            return true;
                        }
                        return false;
                    }
                }
        );
    //    filter.print();


        KeyedStream<Config, Integer> keyBy = streamOperator.keyBy(new KeySelector<Config, Integer>() {
            @Override
            public Integer getKey(Config config) throws Exception {
                return config.getAge();
            }
        });
      //  keyBy.print();

        //Reduce 返回单个的结果值，并且 reduce 操作每处理一个元素总是创建一个新值。
        // 常用的方法有 average, sum, min, max, count，使用 reduce 方法都可实现。
        SingleOutputStreamOperator<Config> reduce = streamOperator.keyBy(
                new KeySelector<Config, Integer>(){
                    @Override
                    public Integer getKey(Config config) throws Exception {
                        return config.getAge();
                    }
                }).reduce(new ReduceFunction<Config>() {
            @Override
            public Config reduce(Config t1, Config t2) throws Exception {
                Config config = new Config();
                config.setLabel(t1.getLabel() + t2.getLabel());
                config.setAge(t1.getAge() + t2.getAge() / 2);
                config.setRemark(t1.getRemark() + t2.getRemark());
                config.setValue(t1.getValue() + t2.getValue());
                config.setVoiceValue(t1.getVoiceValue() + t2.getVoiceValue());
                return config;
            }
        });
        reduce.print();

        //    student.addSink(new SinkToMySQL()); //数据 sink 到 mysql
        env.execute("Flink add sink");
    }
}
