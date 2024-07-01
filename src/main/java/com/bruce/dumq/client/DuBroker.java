package com.bruce.dumq.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * broker for topics.
 * @date 2024/6/25
 */
public class DuBroker {

    Map<String, DuMq> mqMapping = new ConcurrentHashMap<>(64);
    public DuMq find(String topic) {
        return mqMapping.get(topic);
    }

    public DuMq createTopic(String topic){
        return mqMapping.putIfAbsent(topic, new DuMq(topic));
    }

    public DuProducer createProduce(){
        return new DuProducer(this);
    }

    public DuConsumer<?> createConsumer(String topic){
        DuConsumer<?> consumer = new DuConsumer<>(this);
        consumer.subscribe(topic);
        return consumer;
    }

}
