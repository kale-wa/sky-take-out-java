package com.sky.mq;

/**
 * RabbitMQ 常量：交换机、队列、路由键集中管理，避免散落在各个类里。
 */
public class MqConstants {

    private MqConstants() {
    }

    /* ================= 交换机 ================= */

    /** 业务交换机：下单后往这里发消息 */
    public static final String ORDER_EXCHANGE = "sky.order.exchange";

    /** 死信交换机：延迟队列里过期的消息会被转发到这里 */
    public static final String ORDER_DLX_EXCHANGE = "sky.order.dlx.exchange";

    /** 失败交换机：消费失败的消息进这里，保留现场 */
    public static final String ORDER_FAIL_EXCHANGE = "sky.order.fail.exchange";

    /* ================= 路由键 ================= */

    public static final String ORDER_DELAY_ROUTING_KEY = "order.delay";
    public static final String ORDER_CANCEL_ROUTING_KEY = "order.cancel";
    public static final String ORDER_NOTIFY_ROUTING_KEY = "order.notify";
    public static final String ORDER_FAIL_ROUTING_KEY = "order.fail";

    /* ================= 队列 ================= */

    /** 延迟队列：没有消费者，只负责等消息过期 */
    public static final String ORDER_DELAY_QUEUE = "sky.order.delay.queue";

    /** 关单队列：延迟队列的死信真正到达这里，被消费者处理 */
    public static final String ORDER_CANCEL_QUEUE = "sky.order.cancel.queue";

    /** 下单事件队列：异步通知、积分等非核心逻辑 */
    public static final String ORDER_NOTIFY_QUEUE = "sky.order.notify.queue";

    /** 失败队列：消费多次仍失败的消息，人工排查 */
    public static final String ORDER_FAIL_QUEUE = "sky.order.fail.queue";
}
