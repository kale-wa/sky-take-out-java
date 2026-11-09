package com.sky.mq.consumer;

import com.rabbitmq.client.Channel;
import com.sky.mq.MqConstants;
import com.sky.mq.OrderMessage;
import com.sky.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 订单超时关单消费者。
 *
 * 可靠性三件套：
 *   1. 手动 ACK：业务成功才确认，失败 nack 且不重回队列（进死信）
 *   2. Redis 幂等：同一条消息只处理一次
 *   3. 业务幂等：Service 里再判断一次订单状态
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderTimeoutConsumer {

    private final OrderService orderService;
    private final StringRedisTemplate stringRedisTemplate;

    @RabbitListener(queues = MqConstants.ORDER_CANCEL_QUEUE)
    public void onOrderTimeout(OrderMessage msg, Channel channel, Message message) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String idempotentKey = "mq:close:" + msg.getOrderId();

        // 幂等第一道：Redis SETNX，重复消息直接确认
        Boolean first = stringRedisTemplate.opsForValue()
                .setIfAbsent(idempotentKey, "1", 7, TimeUnit.DAYS);
        if (Boolean.FALSE.equals(first)) {
            log.info("[超时关单] 重复消息，直接确认, orderId={}", msg.getOrderId());
            channel.basicAck(deliveryTag, false);
            return;
        }

        try {
            orderService.cancelTimeoutOrder(msg.getOrderId());
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            log.error("[超时关单] 处理失败, orderId={}", msg.getOrderId(), e);
            // 失败时删掉幂等键，允许后续补偿时重新处理
            stringRedisTemplate.delete(idempotentKey);
            // requeue=false：不重回原队列，避免死循环，交给死信队列人工处理
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
