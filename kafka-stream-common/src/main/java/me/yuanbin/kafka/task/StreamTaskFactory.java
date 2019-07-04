package me.yuanbin.kafka.task;

import com.google.common.base.Strings;
import com.typesafe.config.Config;
import me.yuanbin.common.config.AppConfigFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author billryan
 * @date 2019-07-04
 */
public class StreamTaskFactory {
    private static final Logger logger = LoggerFactory.getLogger(StreamTaskFactory.class);
    private static final String APPLICATION_ID_KEY = "application.id";
    private static final String DEFAULT_APPLICATION_ID = "kafka.application.id";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    private static final Config APP_CONFIG = AppConfigFactory.load();

    private static final Map<String, StreamsBuilder> TASKS_BUILDER = new ConcurrentHashMap<>();

    private static Properties getKafkaProps(String appId, String bootstrapServers) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    private static Properties getKafkaProps(String appId) {
        String finalAppId = Strings.isNullOrEmpty(appId) ? APP_CONFIG.getString(DEFAULT_APPLICATION_ID) : appId;
        String bootstrapServers = APP_CONFIG.getString(DEFAULT_BOOTSTRAP_SERVERS);
        return getKafkaProps(finalAppId, bootstrapServers);
    }

    private static String getAppId(StreamTask streamTask) {
        final Config taskConfig = APP_CONFIG.getConfig("task." + streamTask.getClass().getSimpleName());
        if (taskConfig.hasPath(APPLICATION_ID_KEY)) {
            return taskConfig.getString(APPLICATION_ID_KEY);
        }
        return APP_CONFIG.getString(DEFAULT_APPLICATION_ID);
    }

    public static void load(StreamTask streamTask) {
        String appId = getAppId(streamTask);
        logger.info("load task {} with application.id {}", streamTask.getClass().getSimpleName(), appId);
        TASKS_BUILDER.putIfAbsent(appId, new StreamsBuilder());
        streamTask.load(TASKS_BUILDER.get(appId));
    }

    /**
     * start at the end of main entry
     */
    public static void start() {
        for (Map.Entry<String, StreamsBuilder> entry : TASKS_BUILDER.entrySet()) {
            String appId = entry.getKey();
            StreamsBuilder taskBuilder = entry.getValue();
            Properties properties = getKafkaProps(appId);
            logger.info("start with application.id: {}", appId);
            KafkaStreams streams = new KafkaStreams(taskBuilder.build(), properties);
            streams.start();
        }
    }
}
