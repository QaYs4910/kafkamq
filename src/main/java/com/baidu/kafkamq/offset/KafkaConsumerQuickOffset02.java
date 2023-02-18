package com.baidu.kafkamq.offset;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaConsumerQuickOffset02 {

    public static void main(String[] args) {

        /**
         * 消费者首次消费时策略:
         * ** 对于首次订阅消费的时没有找到该消费分区的记录,默认的kafka消费者首次的消费的默认策略:latest(自动将偏移量重置为最新的偏移量)
         *
         * 1. earliest: 自动将偏移量重置为最早的偏移量
         * 2. latest: 自动将偏移量重置为最新的偏移量
         * 3. x: 如果没有找到之前的消费者组的先前的偏移量,则向消费者抛异常
         */
        Properties properties = new Properties();
        //连接配置需要在本地设置hosts文件对应地址的关系
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"CentOSA:9092,CentOSB:9092,CentOSC:9092");
        //消费信息需要进行反序列化配置
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //消费者组的信息
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"g2");
        /**
         * 定义消费策略
         * 可以获取该topic中以前的所有消息
         */
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //1.创建kafka的Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //2.订阅
        consumer.subscribe(Pattern.compile("topic01"));
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
