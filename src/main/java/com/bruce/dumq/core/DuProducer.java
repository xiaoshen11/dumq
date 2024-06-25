package com.bruce.dumq.core;

import lombok.AllArgsConstructor;

/**
 * message queue producer
 * @date 2024/6/25
 */
@AllArgsConstructor
public class DuProducer {

    DuBroker broker;

    public boolean send(String topic, DuMessage message){
        DuMq mq = broker.find(topic);
        if(mq == null){
            throw new RuntimeException("topic not found");
        }
        return mq.send(message);
    }

}
