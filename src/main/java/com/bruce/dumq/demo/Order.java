package com.bruce.dumq.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @date 2024/6/25
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Order {

    private long id;
    private String item;
    private double price;

}
