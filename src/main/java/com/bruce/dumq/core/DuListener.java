package com.bruce.dumq.core;

/**
 * @date 2024/6/25
 */
public interface DuListener<T> {

    void onMessage(DuMessage<T> message);
}
