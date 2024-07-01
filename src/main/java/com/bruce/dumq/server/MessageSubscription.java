package com.bruce.dumq.server;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @date 2024/7/1
 */
@Data
@AllArgsConstructor
public class MessageSubscription {

    private String topic;
    private String consumerId;
    private int offset = -1;

}
