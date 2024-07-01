package com.bruce.dumq.client;

import com.bruce.dumq.model.DuMessage;

/**
 * @date 2024/6/25
 */
public interface DuListener<T> {

    void onMessage(DuMessage<T> message);
}
