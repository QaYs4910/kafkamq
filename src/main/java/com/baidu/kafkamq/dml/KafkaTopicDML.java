package com.baidu.kafkamq.dml;

import org.apache.kafka.clients.admin.*;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaTopicDML {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1.创建kafkaAdminClient
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"CentOSA:9092,CentOSB:9092,CentOSC:9092");
        KafkaAdminClient adminClient = (KafkaAdminClient)KafkaAdminClient.create(properties);
        System.out.println("客户端输出:"+adminClient);
        /**
         *  获取topic时为异步
         *  topics.all().get(); 改为同步
         */
        //创建topic
//        CreateTopicsResult topics = adminClient.createTopics(Arrays.asList(new NewTopic("topic02", 3, (short) 3)));
//        topics.all().get();
//
//        //删除topic
//        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList("topic01", "topic02"));
//        deleteTopicsResult.all().get(); //同步方式
//
//        //获取topic详细信息
        DescribeTopicsResult topic05 = adminClient.describeTopics(Arrays.asList("topic01"));
        Map<String, TopicDescription> stringTopicDescriptionMap = topic05.all().get();
        for (Map.Entry<String,TopicDescription> entry:
                stringTopicDescriptionMap.entrySet()) {
            System.out.println(entry.getKey()+":"+entry.getValue());
        }
//        //获取topic列表
        ListTopicsResult topicsResult = adminClient.listTopics();
        Set<String> stringSet = topicsResult.names().get();
        stringSet.forEach(System.out::println);
        //2.关闭
        adminClient.close();
    }

}
