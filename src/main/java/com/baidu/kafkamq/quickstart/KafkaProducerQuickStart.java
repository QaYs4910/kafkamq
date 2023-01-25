package com.baidu.kafkamq.quickstart;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerQuickStart {

    public static void main(String[] args) {

        Properties properties = new Properties();
        //连接配置需要在本地设置hosts文件对应地址的关系
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"CentOSA:9092,CentOSB:9092,CentOSC:9092");
        //配置序列化key and value
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //生产者
        KafkaProducer kafkaProducer = new KafkaProducer<>(properties);
        //生产消息
        for (int i = 0; i < 10; i++) {
            //消息对象
            ProducerRecord<String, String> record = new ProducerRecord<>("topic01", "key" + 1, "value" + 1);
            kafkaProducer.send(record);
        }
        kafkaProducer.close();
    }
}
