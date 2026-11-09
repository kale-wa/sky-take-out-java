package com.sky.config;

import com.sky.mq.MqConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ 拓扑与模板配置。
 *
 * 链路：
 *   下单 --> order.exchange --(order.delay)--> 延迟队列（无消费者，等 TTL 到期）
 *        --(死信)--> order.dlx.exchange --(order.cancel)--> 关单队列 --> 消费者关单
 *
 * 关单队列消费失败（nack 且不重回队列）--> order.fail.exchange --> 失败队列（人工处理）
 */
@Slf4j
@Configuration
public class RabbitMQConfig {

    /* ================= 业务交换机 ================= */

    @Bean
    public DirectExchange orderExchange() {
        return ExchangeBuilder.directExchange(MqConstants.ORDER_EXCHANGE).durable(true).build();
    }

    /* ================= 延迟队列（没有消费者） ================= */

    @Bean
    public Queue orderDelayQueue() {
        return QueueBuilder.durable(MqConstants.ORDER_DELAY_QUEUE)
                // 消息过期后转发到死信交换机
                .withArgument("x-dead-letter-exchange", MqConstants.ORDER_DLX_EXCHANGE)
                .withArgument("x-dead-letter-routing-key", MqConstants.ORDER_CANCEL_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding orderDelayBinding() {
        return BindingBuilder.bind(orderDelayQueue())
                .to(orderExchange())
                .with(MqConstants.ORDER_DELAY_ROUTING_KEY);
    }

    /* ================= 死信交换机 + 关单队列 ================= */

    @Bean
    public DirectExchange orderDlxExchange() {
        return ExchangeBuilder.directExchange(MqConstants.ORDER_DLX_EXCHANGE).durable(true).build();
    }

    @Bean
    public Queue orderCancelQueue() {
        return QueueBuilder.durable(MqConstants.ORDER_CANCEL_QUEUE)
                // 消费失败（basicNack + requeue=false）后转入失败队列
                .withArgument("x-dead-letter-exchange", MqConstants.ORDER_FAIL_EXCHANGE)
                .withArgument("x-dead-letter-routing-key", MqConstants.ORDER_FAIL_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding orderCancelBinding() {
        return BindingBuilder.bind(orderCancelQueue())
                .to(orderDlxExchange())
                .with(MqConstants.ORDER_CANCEL_ROUTING_KEY);
    }

    /* ================= 失败队列 ================= */

    @Bean
    public DirectExchange orderFailExchange() {
        return ExchangeBuilder.directExchange(MqConstants.ORDER_FAIL_EXCHANGE).durable(true).build();
    }

    @Bean
    public Queue orderFailQueue() {
        return QueueBuilder.durable(MqConstants.ORDER_FAIL_QUEUE).build();
    }

    @Bean
    public Binding orderFailBinding() {
        return BindingBuilder.bind(orderFailQueue())
                .to(orderFailExchange())
                .with(MqConstants.ORDER_FAIL_ROUTING_KEY);
    }

    /* ================= 下单事件队列 ================= */

    @Bean
    public Queue orderNotifyQueue() {
        return QueueBuilder.durable(MqConstants.ORDER_NOTIFY_QUEUE).build();
    }

    @Bean
    public Binding orderNotifyBinding() {
        return BindingBuilder.bind(orderNotifyQueue())
                .to(orderExchange())
                .with(MqConstants.ORDER_NOTIFY_ROUTING_KEY);
    }

    /* ================= 消息转换器 ================= */

    @Bean
    public MessageConverter jacksonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    /* ================= RabbitTemplate：生产端可靠性 ================= */

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory,
                                         MessageConverter jacksonMessageConverter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jacksonMessageConverter);
        // 消息无法路由时回调（需要 publisher-returns: true）
        template.setMandatory(true);

        // 消息是否到达 Broker：ack=false 说明投递失败
        template.setConfirmCallback((correlationData, ack, cause) -> {
            if (!ack) {
                log.error("[MQ] 消息未到达 Broker, correlationData={}, cause={}", correlationData, cause);
                // 生产环境：落库 + 定时补偿，或告警
            }
        });

        // 交换机存在但没有匹配队列时触发
        template.setReturnsCallback(returned ->
                log.error("[MQ] 消息无法路由, exchange={}, routingKey={}",
                        returned.getExchange(), returned.getRoutingKey()));

        return template;
    }
}
