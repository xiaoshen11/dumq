package com.bruce.dumq.demo;

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

        String topic = "du.order";
        DuBroker broker = new DuBroker();
        broker.createTopic(topic);

        DuProducer producer = broker.createProduce();
        DuConsumer<?> consumer = broker.createConsumer(topic);
        consumer.subscribe(topic);
        consumer.listen(message -> {
            System.out.println(" onMessage => " + message);
        });

        for (int i = 0; i < 10; i++) {
            Order order = new Order(ids, "item" + ids, 100*ids);
             producer.send(topic,new DuMessage<>(ids++, order,null));
        }

        for (int i = 0; i < 10; i++) {
            DuMessage<Order> message = (DuMessage<Order>)consumer.poll(1000);
            System.out.println(message);
        }

        while (true){
            char c = (char)System.in.read();
            if(c == 'q' || c == 'e'){
                break;
            }
            if(c == 'p'){
                Order order = new Order(ids, "item" + ids, 100*ids);
                producer.send(topic,new DuMessage<>((long)ids++, order,null));
                System.out.println("send ok => " + order);
            }
            if(c == 'c'){
                DuMessage<Order> message = (DuMessage<Order>)consumer.poll(1000);
                System.out.println("poll ok => " + message);
            }
            if(c == 'a'){
                for (int i = 0; i < 10; i++) {
                    Order order = new Order(ids, "item" + ids, 100*ids);
                    producer.send(topic,new DuMessage<>(ids++, order,null));
                }
                System.out.println("send 10 orders...");
            }
        }

    }

}
