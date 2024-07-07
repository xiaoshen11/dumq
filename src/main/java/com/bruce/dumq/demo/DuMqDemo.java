package com.bruce.dumq.demo;

import com.alibaba.fastjson.JSON;
import com.bruce.dumq.client.DuBroker;
import com.bruce.dumq.client.DuConsumer;
import com.bruce.dumq.model.DuMessage;
import com.bruce.dumq.client.DuProducer;
import lombok.SneakyThrows;

/**
 * @date 2024/6/25
 */
public class DuMqDemo {

    @SneakyThrows
    public static void main(String[] args) {

        long ids = 0;

        String topic = "com.bruce.test";
        DuBroker broker = DuBroker.getDefault();
//        broker.createTopic(topic);

        DuProducer producer = broker.createProduce();
        DuConsumer<?> consumer = broker.createConsumer(topic);
//        consumer.sub(topic);
        consumer.listen(topic, message -> {
            System.out.println(" onMessage => " + message);
        });

//        DuConsumer<?> consumer1 = broker.createConsumer(topic);
//        consumer1.sub(topic);

        for (int i = 0; i < 10; i++) {
            Order order = new Order(ids, "item" + ids, 100*ids);
            producer.send(topic,new DuMessage<>(ids++, JSON.toJSONString(order),null));
        }

//        for (int i = 0; i < 10; i++) {
//            DuMessage<String> message = (DuMessage<String>)consumer1.recv(topic);
//            System.out.println(message);
//            consumer1.ack(topic,message);
//        }

        while (true){
            char c = (char)System.in.read();
            if(c == 'q' || c == 'e'){
                break;
            }
            if(c == 'p'){
                Order order = new Order(ids, "item" + ids, 100*ids);
                producer.send(topic,new DuMessage<>((long)ids++, JSON.toJSONString(order),null));
                System.out.println("produce ok => " + order);
            }
            if(c == 'c'){
//                DuMessage<String> message = (DuMessage<String>)consumer1.recv(topic);
//                System.out.println("consume ok => " + message);
//                consumer1.ack(topic,message);
            }
            if(c == 'a'){
                for (int i = 0; i < 10; i++) {
                    Order order = new Order(ids, "item" + ids, 100*ids);
                    producer.send(topic,new DuMessage<>(ids++, JSON.toJSONString(order),null));
                }
                System.out.println("produce 10 orders...");
            }
        }

    }

}
