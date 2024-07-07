package com.bruce.dumq.client;

import com.bruce.dumq.model.DuMessage;
import lombok.AllArgsConstructor;

/**
 * message queue producer
 * @date 2024/6/25
 */
@AllArgsConstructor
public class DuProducer {

    DuBroker broker;

    public boolean send(String topic, DuMessage message){
        return broker.send(topic, message);
    }

}
