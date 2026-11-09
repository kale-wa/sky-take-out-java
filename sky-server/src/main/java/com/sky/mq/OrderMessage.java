package com.sky.mq;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 订单消息体。
 *
 * 注意：字段只用 Long / String 这类简单类型。
 * 如果放 LocalDateTime，默认的 Jackson2JsonMessageConverter 反序列化会报错，
 * 新手最容易卡在这里。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 订单 id */
    private Long orderId;

    /** 订单号 */
    private String orderNumber;

    /** 下单用户 id */
    private Long userId;
}
