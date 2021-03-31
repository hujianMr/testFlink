package com.fadada.log;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

public class Test {

    static String str = "{\"@timestamp\":#timestamp#,\"log\":\"#timestampStr# | /contract/rec-sign | #requestId# | ERROR | paas-service-contract | contract-dp-56bf69777d-5rz9d | [http-nio-8890-exec-41] | c.f.c.c.s.i.c.ContractRecSignServiceImpl | [TxId] : api-signtask-service^1615885704473^621129 # [SpanId] : -4053522222648396863 | 远程签章服务失败:签章不存在\"}";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.19.64.26:9092");
        props.put("acks", "0");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String a = str.replace("#timestamp#", System.currentTimeMillis()+"");
        a = a.replace("#timestampStr#", dateFormatter.format(new Date()));
        a = a.replace("#requestId#", UUID.randomUUID().toString());
        ProducerRecord record = new ProducerRecord<String, String>(
                "test", null, null, a);
        System.out.println(a);
        producer.send(record);
        producer.flush();
    }
}
