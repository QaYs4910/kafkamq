package com.baidu.kafkamq.idempotence;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerIdempotence {

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"CentOSA:9092,CentOSB:9092,CentOSC:9092");
        //配置序列化key and value
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /**
         * 设置acks和retries
         */
        properties.put(ProducerConfig.ACKS_CONFIG,"all");//消费者主从同步完成响应
        properties.put(ProducerConfig.RETRIES_CONFIG,3); //超时重试次数
        //配置请求超时时间
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,1); //单位毫秒
        /*
         * 开启kafka密等性
         * 1.acks 必须为all
         * 2.retries必须大于0
         */
        //是否开启密等性
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
        //如果又一个发送不成功就阻塞
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,1);

        //生产者
        KafkaProducer kafkaProducer = new KafkaProducer<>(properties);
        //生产消息
        for (int i = 0; i < 1; i++) {
            //消息对象
            ProducerRecord<String, String> record = new ProducerRecord<>("topic01", "idempotence", "idempotence");
            //ProducerRecord<String, String> record = new ProducerRecord<>("topic01",  "value" +i);
            kafkaProducer.send(record);
            kafkaProducer.flush();
        }
        kafkaProducer.close();
    }
}
