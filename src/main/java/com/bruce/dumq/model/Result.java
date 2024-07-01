package com.bruce.dumq.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @date 2024/7/1
 */
@Data
@AllArgsConstructor
public class Result<T> {

    private int code;// 0 fail 1 success
    private T data;

    public static Result ok() {
        return new Result(1,"OK");
    }

    public static Result ok(String msg) {
        return new Result(1,msg);
    }

    public static Result msg(String msg) {
        return new Result(1,DuMessage.create(msg));
    }

    public static Result msg(DuMessage<?> msg) {
        return new Result(1,msg);
    }
}
