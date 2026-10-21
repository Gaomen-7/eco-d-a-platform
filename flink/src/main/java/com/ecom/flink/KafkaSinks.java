package com.ecom.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * Kafka sink 集中处。
 *
 * <p>DLQ 用 <b>至少一次</b>而不是精确一次：DLQ 是「取证通道」，宁可重也不可丢；
 * 而幂等性由「重放时同一 event_id 会被去重段拦下、根本不会再进 DLQ」保证
 * —— 这正是 verify-fault「DLQ 增量 == 0」能成立的原因（票面 ④）。
 *
 * <p>★ 注意返回类型写成 {@link SingleOutputStreamOperator}：
 * {@code KafkaSink} 实现的是 {@code Sink<InputT>}（connector-base 的通用 sink 接口），
 * 走的是 {@code DataStream.sinkTo()} 重载；Declarative 的旧 {@code SinkFunction}
 * 路径不接受它。保留具体类型可让编译期的重载解析确定。
 */
public final class KafkaSinks {

    private KafkaSinks() {}

    static KafkaSink<String> dlq() {
        return KafkaSink.<String>builder()
                .setBootstrapServers(JobConfig.KAFKA_BOOTSTRAP)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(JobConfig.TOPIC_DLQ)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                // 不显式设 deliveryGuarantee：KafkaSinkBuilder 的**默认值就是
                // AT_LEAST_ONCE**（DLQ 不能丢，重复可容忍；去重段保证重放不进 DLQ）。
                // 显式设需要 org.apache.flink.connector.base.DeliveryGuarantee，
                // 而 Flink 2.2 的 flink-connector-base 无 2.2.1 发布版 ⇒ 反成负担。
                .build();
    }

    /** 把 DLQ 流真正接上 sink（无 sink 的算子会被 Flink 从作业图剪掉）。 */
    static void attachDlq(DataStream<String> payload) {
        payload.sinkTo(dlq()).name("dlq-sink").uid("dlq-sink");
    }
}

