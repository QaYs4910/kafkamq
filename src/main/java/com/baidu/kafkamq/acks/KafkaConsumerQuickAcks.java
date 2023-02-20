package com.baidu.kafkamq.acks;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaConsumerQuickAcks {
    /**
     * kafka手动关闭自动提交,自定义提交偏移量
     * @param args
     */
    public static void main(String[] args) {

        /**
         * 消费者首次消费时策略:
         * ** 对于首次订阅消费的时没有找到该消费分区的记录,默认的kafka消费者首次的消费的默认策略:latest(自动将偏移量重置为最新的偏移量)
         *
         * 1. earliest: 自动将偏移量重置为最早的偏移量
         * 2. latest: 自动将偏移量重置为最新的偏移量
         * 3. none: 如果没有找到之前的消费者组的先前的偏移量,则向消费者抛异常
         */
        Properties properties = new Properties();
        //连接配置需要在本地设置hosts文件对应地址的关系
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"CentOSA:9092,CentOSB:9092,CentOSC:9092");
        //消费信息需要进行反序列化配置
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //消费者组的信息
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"g4");

        /**
         * AUTO_COMMIT_INTERVAL_MS_CONFIG 配置自动提交offset 的时间
         * ENABLE_AUTO_COMMIT_CONFIG 开启自动提交offset
         */
        //获取订阅topic以前的所有的消息
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        //自动关闭offset提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

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

                //记录消费元数据的信息
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

                //遍历每个消息的信息
                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                while(iterator.hasNext()){
                    //获取一个消费的消息
                    ConsumerRecord<String, String> next = iterator.next();
                    String topic = next.topic();
                    int partition = next.partition();
                    long offset = next.offset();
                    String key = next.key();
                    String value = next.value();
                    long timestamp = next.timestamp();
                    //元数据
                    offsets.put(new TopicPartition(topic,partition),new OffsetAndMetadata(offset+1));
                    //异步提交偏移量
                    consumer.commitAsync(offsets, new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                            System.out.println("offsets:"+offsets+"----"+"exception:"+e);
                        }
                    });
                    System.out.println("topic:"+topic+","+"partition:"+partition+","+"key:"+key+","+"value:"+value+","+"timestamp:"+timestamp);
                }
            }
        }
    }
}
