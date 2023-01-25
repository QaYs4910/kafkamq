package com.baidu.kafkamq.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class UserDefinePartitioner implements Partitioner {
    private AtomicInteger atomicInteger =  new AtomicInteger();

    /**
     * 返回的分区号
     * @param topic
     * @param key
     * @param keyBytes
     * @param value
     * @param valueBytes
     * @param cluster
     * @return
     */
    @Override
    public int partition(String topic,
                         Object key, //key
                         byte[] keyBytes, //key的字节
                         Object value, //value
                         byte[] valueBytes, //value的字节
                          Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartition = partitions.size();
        if(keyBytes == null){
            int increment = atomicInteger.incrementAndGet();
            return (increment & Integer.MAX_VALUE)%numPartition;
        }else {
            return Utils.toPositive(Utils.murmur2(keyBytes))%numPartition;
        }
    }

    @Override
    public void close() {
        System.out.println("close");
    }

    @Override
    public void configure(Map<String, ?> map) {
        System.out.println("configure");
    }
}
