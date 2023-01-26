package com.baidu.kafkamq.seriailzer;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

public class UserDefineSerializer implements Serializer<Object> {
    //对象序列化
    @Override
    public void configure(Map map, boolean b) {
        System.out.println("configure");
    }
    //序列化
    @Override
    public byte[] serialize(String s, Object data) {
        return SerializationUtils.serialize((Serializable) data); //需要转换的对象需要实现的接口
    }

    @Override
    public void close() {
        System.out.println("close");
    }
}
