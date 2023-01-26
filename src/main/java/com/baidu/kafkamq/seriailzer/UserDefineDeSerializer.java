package com.baidu.kafkamq.seriailzer;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class UserDefineDeSerializer implements Deserializer<Object> {
    // 对象实现反序列化
    @Override
    public void configure(Map<String, ?> map, boolean b) {
        System.out.println("configure");
    }

    @Override
    public Object deserialize(String s, byte[] data) {
        return SerializationUtils.deserialize(data);
    }

    @Override
    public void close() {
        System.out.println("close");
    }
}
