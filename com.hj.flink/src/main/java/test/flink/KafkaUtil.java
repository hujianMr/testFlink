package test.flink;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class KafkaUtil {

    private static KafkaProducer producer;

    static{
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.19.64.26:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        producer = new KafkaProducer<String, String>(props);
    }

    public static KafkaProducer get(){
        return producer;
    }
}
