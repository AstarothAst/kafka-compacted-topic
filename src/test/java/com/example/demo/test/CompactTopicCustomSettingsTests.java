package com.example.demo.test;

import com.example.demo.AbstractKafkaTest;
import com.example.demo.TopicConfigBuilder;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.springframework.kafka.config.TopicBuilder;
import org.testcontainers.shaded.com.google.common.collect.ArrayListMultimap;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT;

@DisplayName("Тесты записи в compacted топик с кастомными настройками")
class CompactTopicCustomSettingsTests extends AbstractKafkaTest {

    static final int REPEAT_NUM = 10;
    static final int MSG_COUNT = 5_000;

    static ArrayListMultimap<String, TestResults> topicToResultMap = ArrayListMultimap.create();

    @AfterAll
    public static void afterAll() {
        logResults(topicToResultMap);
    }

    @RepeatedTest(REPEAT_NUM)
    @DisplayName("Компактный топик 5000:100, 0% дублирующих ключей")
    void test_10() {
        //given
        String topicName = "topic1";
        long maxCompactionLagMs = 5000L;
        long deleteRetentionMs = 100L;
        int msgCount = MSG_COUNT;
        int percentage = 0;

        List<KafkaRecord> messages = generateKafkaMessages(msgCount, percentage);

        Map<String, String> config = TopicConfigBuilder.cleanupPolicy(CLEANUP_POLICY_COMPACT)
                .setMaxCompactionLagMs(maxCompactionLagMs)
                .setDeleteRetentionMs(deleteRetentionMs)
                .build();

        NewTopic topic = TopicBuilder.name(topicName).configs(config).build();
        createTopic(topic);

        //when
        Duration duration = fillTopicAndGetDuration(topicName, messages);

        //then
        fillStatistics(topicToResultMap, topicName, msgCount, percentage, duration, config);
    }

    @RepeatedTest(REPEAT_NUM)
    @DisplayName("Компактный топик 5000:100, 50% дублирующих ключей")
    void test_20() {
        //given
        String topicName = "topic2";
        long maxCompactionLagMs = 5000L;
        long deleteRetentionMs = 100L;
        int msgCount = MSG_COUNT;
        int percentage = 50;

        List<KafkaRecord> messages = generateKafkaMessages(msgCount, percentage);

        Map<String, String> config = TopicConfigBuilder.cleanupPolicy(CLEANUP_POLICY_COMPACT)
                .setMaxCompactionLagMs(maxCompactionLagMs)
                .setDeleteRetentionMs(deleteRetentionMs)
                .build();

        NewTopic topic = TopicBuilder.name(topicName).configs(config).build();
        createTopic(topic);

        //when
        Duration duration = fillTopicAndGetDuration(topicName, messages);

        //then
        fillStatistics(topicToResultMap, topicName, msgCount, percentage, duration, config);
    }

    @RepeatedTest(REPEAT_NUM)
    @DisplayName("Компактный топик 5000:100, 100% дублирующих ключей")
    void test_30() {
        //given
        String topicName = "topic3";
        long maxCompactionLagMs = 5000L;
        long deleteRetentionMs = 100L;
        int msgCount = MSG_COUNT;
        int percentage = 100;

        List<KafkaRecord> messages = generateKafkaMessages(msgCount, percentage);

        Map<String, String> config = TopicConfigBuilder.cleanupPolicy(CLEANUP_POLICY_COMPACT)
                .setMaxCompactionLagMs(maxCompactionLagMs)
                .setDeleteRetentionMs(deleteRetentionMs)
                .build();

        NewTopic topic = TopicBuilder.name(topicName).configs(config).build();
        createTopic(topic);

        //when
        Duration duration = fillTopicAndGetDuration(topicName, messages);

        //then
        fillStatistics(topicToResultMap, topicName, msgCount, percentage, duration, config);
    }

    @RepeatedTest(REPEAT_NUM)
    @DisplayName("Компактный топик 20000:20000, 0% дублирующих ключей")
    void test_40() {
        //given
        String topicName = "topic4";
        long maxCompactionLagMs = 20000;
        long deleteRetentionMs = 20000;
        int msgCount = MSG_COUNT;
        int percentage = 0;

        List<KafkaRecord> messages = generateKafkaMessages(msgCount, percentage);

        Map<String, String> config = TopicConfigBuilder.cleanupPolicy(CLEANUP_POLICY_COMPACT)
                .setMaxCompactionLagMs(maxCompactionLagMs)
                .setDeleteRetentionMs(deleteRetentionMs)
                .build();

        NewTopic topic = TopicBuilder.name(topicName).configs(config).build();
        createTopic(topic);

        //when
        Duration duration = fillTopicAndGetDuration(topicName, messages);

        //then
        fillStatistics(topicToResultMap, topicName, msgCount, percentage, duration, config);
    }

    @RepeatedTest(REPEAT_NUM)
    @DisplayName("Компактный топик 2000:2000, 50% дублирующих ключей")
    void test_50() {
        //given
        String topicName = "topic5";
        long maxCompactionLagMs = 20000;
        long deleteRetentionMs = 20000;
        int msgCount = MSG_COUNT;
        int percentage = 50;

        List<KafkaRecord> messages = generateKafkaMessages(msgCount, percentage);

        Map<String, String> config = TopicConfigBuilder.cleanupPolicy(CLEANUP_POLICY_COMPACT)
                .setMaxCompactionLagMs(maxCompactionLagMs)
                .setDeleteRetentionMs(deleteRetentionMs)
                .build();

        NewTopic topic = TopicBuilder.name(topicName).configs(config).build();
        createTopic(topic);

        //when
        Duration duration = fillTopicAndGetDuration(topicName, messages);

        //then
        fillStatistics(topicToResultMap, topicName, msgCount, percentage, duration, config);
    }

    @RepeatedTest(REPEAT_NUM)
    @DisplayName("Компактный топик 2000:2000, 100% дублирующих ключей")
    void test_60() {
        //given
        String topicName = "topic6";
        long maxCompactionLagMs = 20000;
        long deleteRetentionMs = 20000;
        int msgCount = MSG_COUNT;
        int percentage = 100;

        List<KafkaRecord> messages = generateKafkaMessages(msgCount, percentage);

        Map<String, String> config = TopicConfigBuilder.cleanupPolicy(CLEANUP_POLICY_COMPACT)
                .setMaxCompactionLagMs(maxCompactionLagMs)
                .setDeleteRetentionMs(deleteRetentionMs)
                .build();

        NewTopic topic = TopicBuilder.name(topicName).configs(config).build();
        createTopic(topic);

        //when
        Duration duration = fillTopicAndGetDuration(topicName, messages);

        //then
        fillStatistics(topicToResultMap, topicName, msgCount, percentage, duration, config);
    }
}
