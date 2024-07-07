package com.bruce.dumq.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * @date 2024/7/1
 */
@Data
@AllArgsConstructor
public class Result<T> {

    private int code;// 0 fail 1 success
    private T data;

    public static Result<String> ok() {
        return new Result(1,"OK");
    }

    public static Result<String> ok(String msg) {
        return new Result(1,msg);
    }

    public static Result<DuMessage<?>> msg(String msg) {
        return new Result(1,DuMessage.create(msg));
    }

    public static Result<DuMessage<?>> msg(DuMessage<?> msg) {
        return new Result(1,msg);
    }

    public static Result<List<DuMessage<?>>> msg(List<DuMessage<?>> msgs) {
        return new Result(1,msgs);
    }
}
