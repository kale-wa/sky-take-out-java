package com.sky.mq.consumer;

import com.rabbitmq.client.Channel;
import com.sky.mq.MqConstants;
import com.sky.mq.OrderMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 下单事件消费者。
 *
 * 演示“异步解耦”：下单主链路只负责写库 + 发消息，
 * 通知用户、加积分、打印小票这类非核心逻辑放到这里做。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderNotifyConsumer {

    private final StringRedisTemplate stringRedisTemplate;

    @RabbitListener(queues = MqConstants.ORDER_NOTIFY_QUEUE)
    public void onOrderCreated(OrderMessage msg, Channel channel, Message message) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String idempotentKey = "mq:notify:" + msg.getOrderId();

        Boolean first = stringRedisTemplate.opsForValue()
                .setIfAbsent(idempotentKey, "1", 1, TimeUnit.DAYS);
        if (Boolean.FALSE.equals(first)) {
            channel.basicAck(deliveryTag, false);
            return;
        }

        try {
            // 真实项目：发短信 / App 推送 / 加积分 / 打印小票
            log.info("[下单事件] 订单 {} 创建成功，提醒用户尽快支付", msg.getOrderNumber());
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            log.error("[下单事件] 处理失败, orderId={}", msg.getOrderId(), e);
            stringRedisTemplate.delete(idempotentKey);
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
