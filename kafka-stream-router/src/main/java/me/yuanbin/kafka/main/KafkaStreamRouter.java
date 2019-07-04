package me.yuanbin.kafka.main;

import me.yuanbin.kafka.task.impl.MaxwellKeyRouterTask;
import me.yuanbin.kafka.task.StreamTaskFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author billryan
 * @date 2019-07-05
 */
public class KafkaStreamRouter {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamRouter.class);

    public static void main(final String[] args) {

        StreamTaskFactory.load(new MaxwellKeyRouterTask());

        StreamTaskFactory.start();
    }
}
