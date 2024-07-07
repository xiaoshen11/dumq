package com.bruce.dumq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * du message model
 * @date 2024/6/25
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DuMessage<T> {

    static AtomicLong idgen = new AtomicLong(0);
    //private String topic;
    private Long id;
    private T body;
    private Map<String, String> headers = new HashMap<>();
//    private Map<String, String> properties;

    public static long getId(){
        return idgen.getAndIncrement();
    }

    public static DuMessage<String> create(String msg){
        return new DuMessage(getId(), msg, null);
    }
}
