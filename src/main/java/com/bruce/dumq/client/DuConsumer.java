package com.bruce.dumq.client;

import com.bruce.dumq.model.DuMessage;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * message consumer
 * @date 2024/6/25
 */
public class DuConsumer<T> {

    private String id;
    DuBroker broker;
    String topic;
    DuMq mq;

    static AtomicInteger idgen = new AtomicInteger(0);

    public DuConsumer(DuBroker broker) {
        this.broker = broker;
        this.id = "CID" + idgen.getAndIncrement();
    }

    public void subscribe(String topic){
        this.topic = topic;
        mq = broker.find(topic);
        if(mq == null){
            throw new RuntimeException("topic not found");
        }
    }

    public DuMessage<T> poll(long timeout){
        return mq.poll(timeout);
    }

    public void listen(DuListener<T> listener){
        mq.addListener(listener);
    }

}
