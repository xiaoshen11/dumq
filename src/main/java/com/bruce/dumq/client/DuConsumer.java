package com.bruce.dumq.client;

import com.bruce.dumq.model.DuMessage;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * message consumer
 * @date 2024/6/25
 */
public class DuConsumer<T> {

    private String id;
    DuBroker broker;

    static AtomicInteger idgen = new AtomicInteger(0);

    public DuConsumer(DuBroker broker) {
        this.broker = broker;
        this.id = "CID" + idgen.getAndIncrement();
    }

    public void sub(String topic){
        broker.sub(topic, id);
    }

    public void unsub(String topic){
        broker.unsub(topic, id);
    }

    public boolean ack(String topic,Integer offset){
        return broker.ack(topic, id, offset);
    }

    public boolean ack(String topic, DuMessage message) {
        Object o = message.getHeaders().get("x-offset");
        if(o == null){
            return false;
        }
        int offset = Integer.parseInt((String) o);
        return ack(topic,  offset);
    }

    public DuMessage<T> recv(String topic){
        return broker.recv(topic, id);
    }

    public void listen(String topic, DuListener<T> listener){
        this.listener = listener;
        broker.addConsumer(topic, this);
    }

    @Getter
    private DuListener listener;
}
