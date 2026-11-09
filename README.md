# 苍穹外卖（学习与二次开发）

> 本项目基于黑马程序员《苍穹外卖》课程源码，用于学习与二次开发实践。
> 基础业务代码来自课程，**本人完成了下方的 RabbitMQ 改造**（见 commit 记录）。

## 我的改造

### 1. 订单超时自动关单
- 下单成功后发送延迟消息（TTL 30 分钟），消息在延迟队列中等待
- 到期后由 Broker 自动死信转发到关单队列，消费者把订单状态置为「已取消」
- 替代定时任务扫表，避免全表扫描，关单延迟误差小

### 2. 异步解耦
- 下单主链路只负责写库 + 发消息，不被通知、积分等非核心逻辑拖慢
- 通知类逻辑由独立消费者异步处理

### 3. 消息可靠性
- 生产端：publisher confirm + returns（路由失败回调）+ 事务提交后发送（afterCommit）
- Broker：交换机、队列 durable，消息持久化
- 消费端：手动 ACK，失败 `nack(requeue=false)` 进死信；Redis `SETNX` 幂等去重

## 消息链路

```
下单
 ├─ order.exchange --(order.delay)--> 延迟队列（无消费者，TTL 到期）
 │        └─ 死信 --> order.dlx.exchange --(order.cancel)--> 关单队列 --> 关单消费者
 │                              └─ 消费失败 --> order.fail.exchange --> 失败队列
 └─ order.exchange --(order.notify)--> 下单事件队列 --> 通知消费者
```

## 技术栈

Spring Boot 2.7.3 · MyBatis · MySQL 8 · Redis · RabbitMQ · WebSocket

## 如何运行

1. 导入 `sky.sql` 建库
2. 启动 RabbitMQ（Docker）：
   ```bash
   docker run -d --name rabbitmq \
     -p 5672:5672 -p 15672:15672 \
     rabbitmq:3-management
   ```
3. 修改 `sky-server/src/main/resources/application-dev.yml` 中的数据库、Redis、RabbitMQ 连接
4. 启动 `SkyApplication`，或在项目根目录执行 `mvn spring-boot:run`

管理台：<http://localhost:15672>（guest / guest），可以看到 4 个队列和 3 个交换机。

## 配置说明

阿里云 OSS、微信支付的密钥均为**占位符**。本地如需使用，请替换为自己的配置，
并且不要把真实密钥提交到仓库（GitHub 的 secret scanning 会拦截）。
