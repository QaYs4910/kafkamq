package com.baidu.kafkamq.transcations;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

public class KafkaConsumerReadCommitted {

    public static void main(String[] args) {
        /**
         * 1.根据key进行取模运算得到的值在分配到可分配的分区上
         * 2.在同一个组中的多个消费者消费
         */
        Properties properties = new Properties();
        //连接配置需要在本地设置hosts文件对应地址的关系
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"CentOSA:9092,CentOSB:9092,CentOSC:9092");
        //消费信息需要进行反序列化配置
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //消费者组的信息
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"g1");
        //设置消费值的消费级别
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");


        //1.创建kafka的Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //2.订阅
        consumer.subscribe(Arrays.asList("topic01"));
        //遍历消息队列取数据
        while(true){
            //每隔多长时间取数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            //判断消费的信息不为空
            if(!records.isEmpty()){
                //遍历每个消息的信息
                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                while(iterator.hasNext()){
                    //获取一个消费的消息
                    ConsumerRecord<String, String> next = iterator.next();
                    String topic = next.topic();
                    int partition = next.partition();
                    String key = next.key();
                    String value = next.value();
                    long timestamp = next.timestamp();
                    System.out.println("topic:"+topic+","+"partition:"+partition+","+"key:"+key+","+"value:"+value+","+"timestamp:"+timestamp);
                }
            }
        }
    }
}
