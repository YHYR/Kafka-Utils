# Kafka-Utils

## 环境依赖

> Kafka 1.0.1
>
> JDK 1.8
>
> kafka-clients 1.0.1
>
> Python 3.6.5
>
> kafka-python 1.4.3

基于Java和Python实现Kafka集中常见的工具类；会不定期的作以补充和完善。

目前包含

+ 指定消费的起始Offset
+ 不停服修改Offset
+ 获取Partition的有效Offset范围
+ 根据时间戳筛选Offset
+ Rebalance监控
+ 消费__consumer_offsets中的消息
+ 消费指定时间窗内产生的所有消息