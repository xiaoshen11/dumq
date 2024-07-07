package com.bruce.dumq.client;

import com.bruce.dumq.model.DuMessage;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @date 2024/6/25
 */
@AllArgsConstructor
public class DuMq {

//    private String topic;
//
//    private LinkedBlockingQueue<DuMessage> queue = new LinkedBlockingQueue();
//    private List<DuListener> listeners = new ArrayList<>();
//
//    public DuMq(String topic) {
//        this.topic = topic;
//    }
//
//    public boolean send(DuMessage message) {
//        boolean offered = queue.offer(message);
//        listeners.forEach(listener -> listener.onMessage(message));
//        return offered;
//    }
//
//    // 拉模式获取消息
//    @SneakyThrows
//    public <T> DuMessage<T> poll(long timeout)  {
//        return queue.poll(timeout, TimeUnit.MILLISECONDS);
//    }
//
//    public <T> void addListener(DuListener<T> listener){
//        listeners.add(listener);
//    }
}
