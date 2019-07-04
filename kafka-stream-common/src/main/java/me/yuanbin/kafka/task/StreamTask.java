package me.yuanbin.kafka.task;

import org.apache.kafka.streams.StreamsBuilder;

/**
 * @author billryan
 * @date 2019-07-04
 */
public interface StreamTask {

    /**
     * Kafka Stream task
     * @param builder StreamsBuilder instance, created by StreamTasksFactory
     */
    void load(StreamsBuilder builder);
}
