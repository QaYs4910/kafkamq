package com.baidu.kafkamq.transcations;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class KafkaProducerITranscations {

    public static void main(String[] args) {

        KafkaProducer<String, String> kafkaProducer = KafkaProducerITranscations.kafkaProducer();

        //1.初始化事物
        kafkaProducer.initTransactions();

        try{
            //2.开启事物
            kafkaProducer.beginTransaction();
            for (int i = 0; i < 10; i++) {
                if(i == 8){
                    int num = 10/0;
                }
                ProducerRecord<String, String> record = new ProducerRecord<>("topic01", "transaction"+i, "transaction"+i);
                kafkaProducer.send(record);
                kafkaProducer.flush();
            }
            //3.事物正常提交事物
            kafkaProducer.commitTransaction();
        }catch (Exception e){
            //4.事物终止
            System.err.println("错误异常"+e);
            kafkaProducer.abortTransaction();
        }finally {
            kafkaProducer.close();
        }

    }

    public static KafkaProducer<String,String> kafkaProducer(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"CentOSA:9092,CentOSB:9092,CentOSC:9092");
        //配置序列化key and value
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //必须配置事物ID必须唯一
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"transactional-id:"+ UUID.randomUUID());
        //配置kafka批处理
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,1024);
        //未满足批处理的数量的等待最大时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);

        //kafka配置重试机制和密等性
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,20000); //请求超时
        //是否开启密等性
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
        //生产者
       return new KafkaProducer<>(properties);
    }
}
