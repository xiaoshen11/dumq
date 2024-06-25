package com.bruce.dumq.core;

/**
 * message consumer
 * @date 2024/6/25
 */
public class DuConsumer<T> {

    DuBroker broker;
    String topic;
    DuMq mq;

    public DuConsumer(DuBroker broker) {
        this.broker = broker;
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
