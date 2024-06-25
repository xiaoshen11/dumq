package com.bruce.dumq.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * du message model
 * @date 2024/6/25
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DuMessage<T> {

    //private String topic;
    private Long id;
    private T body;
    private Map<String, String> headers;
//    private Map<String, String> properties;


}
