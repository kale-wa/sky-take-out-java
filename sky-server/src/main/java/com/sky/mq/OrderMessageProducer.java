package com.sky.mq;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 订单消息生产者。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderMessageProducer {

    private final RabbitTemplate rabbitTemplate;

    /**
     * 超时时间（毫秒）。测试时设 60000（1 分钟），上线改 1800000（30 分钟）。
     */
    @Value("${sky.mq.order-timeout-ms:1800000}")
    private long orderTimeoutMs;

    /**
     * 发送下单事件：异步通知、积分等非核心逻辑。
     */
    public void sendOrderCreated(OrderMessage message) {
        rabbitTemplate.convertAndSend(
                MqConstants.ORDER_EXCHANGE,
                MqConstants.ORDER_NOTIFY_ROUTING_KEY,
                message);
        log.info("[MQ] 发送下单事件, orderId={}", message.getOrderId());
    }

    /**
     * 发送延迟关单消息。
     *
     * 消息先进入延迟队列，队列没有消费者，等 TTL 到期后由 Broker 自动
     * 转发到死信交换机，最终到达关单队列。
     */
    public void sendOrderDelayClose(OrderMessage message) {
        CorrelationData correlationData = new CorrelationData(String.valueOf(message.getOrderId()));
        rabbitTemplate.convertAndSend(
                MqConstants.ORDER_EXCHANGE,
                MqConstants.ORDER_DELAY_ROUTING_KEY,
                message,
                msg -> {
                    // 每条消息单独设置 TTL（毫秒）。同一队列里 TTL 必须一致，
                    // 否则短 TTL 的消息会被长 TTL 的堵住（队头阻塞）。
                    msg.getMessageProperties().setExpiration(String.valueOf(orderTimeoutMs));
                    // 持久化，Broker 重启后消息还在
                    msg.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    return msg;
                },
                correlationData);
        log.info("[MQ] 发送延迟关单, orderId={}, ttl={}ms", message.getOrderId(), orderTimeoutMs);
    }
}
