package com.bruce.dumq.server;

import com.bruce.dumq.model.DuMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @date 2024/7/1
 */
public class MessageQueue {

    public static final Map<String, MessageQueue> queues = new HashMap<>();
    public static final String TEST_TOPIC = "com.bruce.test";
    static{
        queues.put(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
    }

    private Map<String, MessageSubscription> subscriptions = new HashMap<>();

    private String topic;
    private DuMessage<?>[] queue = new DuMessage[1024*10];
    private int index;

    public MessageQueue(String topic) {
        this.topic = topic;
    }

    public int send(DuMessage<?> message){
        if(index >= queue.length){
            return -1;
        }
        queue[index++] = message;
        return index;
    }

    public DuMessage<?> recv(int ind){
        if(ind < index){
            return queue[ind];
        }
        return null;
    }

    public void subscribe(MessageSubscription subscription){
        String consumerId = subscription.getConsumerId();
        subscriptions.putIfAbsent(consumerId,subscription);
    }

    public void unsubscribe(MessageSubscription subscription){
        String consumerId = subscription.getConsumerId();
        subscriptions.remove(consumerId);
    }

    public static void sub(MessageSubscription subscription){
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if(messageQueue == null){
            throw new RuntimeException("topic not found");
        }
        messageQueue.subscribe(subscription);
    }

    public static void unsub(MessageSubscription subscription){
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if(messageQueue == null){
            return;
        }
        messageQueue.unsubscribe(subscription);
    }

    public static int send(String topic, String consumerId, DuMessage<String> message){
        MessageQueue messageQueue = queues.get(topic);
        if(messageQueue == null){
            throw new RuntimeException("topic not found");
        }
        return messageQueue.send(message);
    }

    // 使用此方法，需要手动调用ack，更新订阅里的offset
    public static DuMessage<?> recv(String topic, String consumerId){
        MessageQueue messageQueue = queues.get(topic);
        if(messageQueue == null){
            throw new RuntimeException("topic not found");
        }
        if(messageQueue.subscriptions.containsKey(consumerId)){
            int ind = messageQueue.subscriptions.get(consumerId).getOffset();
            return messageQueue.recv(ind);
        }
        throw new RuntimeException("subscription not found for topic/consumerId " + topic + "/" + consumerId);
    }

    public static int ack(String topic, String consumerId, int offset){
        MessageQueue messageQueue = queues.get(topic);
        if(messageQueue == null){
            throw new RuntimeException("topic not found");
        }
        if(messageQueue.subscriptions.containsKey(consumerId)){
            MessageSubscription subscription = messageQueue.subscriptions.get(consumerId);
            if(offset >= subscription.getOffset() && offset <= messageQueue.index){
                subscription.setOffset(offset);
                return offset;
            }
            return -1;
        }
        throw new RuntimeException("subscription not found for topic/consumerId " + topic + "/" + consumerId);
    }
}
