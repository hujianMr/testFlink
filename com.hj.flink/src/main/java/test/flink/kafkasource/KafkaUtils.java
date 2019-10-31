package test.flink.kafkasource;

import com.alibaba.fastjson.JSON;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.kafka.clients.producer.ProducerRecord;
import test.flink.KafkaUtil;
import test.flink.model.Metric;

import java.util.Map;

public class KafkaUtils {
    public static final String topic = "metric";  // kafka topic，Flink 程序中需要和这个统一

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(500);
            writeToKafka();
        }
    }

    public static void writeToKafka() throws InterruptedException {

        Metric metric = new Metric();
        metric.setTimestamp(System.currentTimeMillis());
        metric.setName("mem");
        Map<String, String> tags = Maps.newHashMap();
        Map<String, Object> fields = Maps.newHashMap();

        tags.put("cluster", "hujian");
        tags.put("host_ip", "127.0.0.1");

        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244873d);
        fields.put("init", 27244873d);

        metric.setTags(tags);
        metric.setFields(fields);

        ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(metric));
        KafkaUtil.get().send(record);
        System.out.println("发送数据: " + JSON.toJSONString(metric));

    }
}
