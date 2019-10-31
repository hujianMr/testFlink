package test.flink.model;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import test.flink.KafkaUtil;


public class KafkaUtils2 {

    public static final String broker_list = "localhost:9092";
    public static final String topic = "alert-metrics";  //kafka topic 需要和 flink 程序用同一个 topic


    public static void writeToKafka() {
        KafkaProducer producer = KafkaUtil.get();
        for (int i = 1; i <= 20; i++) {
            Config config = new Config("法"+i, "大大"+i, "法大大"+i, ""+i);
            config.setAge(i%3);
            ProducerRecord record = new ProducerRecord<String, String>(
                    topic, null, null, JSON.toJSONString(config));
            producer.send(record);
            System.out.println("发送数据: " + JSON.toJSONString(config));
        }
        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        writeToKafka();
    }
}
