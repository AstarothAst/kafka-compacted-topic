package com.example.demo;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.shaded.com.google.common.collect.ArrayListMultimap;

import java.time.Duration;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.DELETE_RETENTION_MS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG;
import static org.awaitility.Awaitility.await;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaTopicTests extends AbstractIntegrationTest {

    public static final int REPEAT_NUM = 5;
    public static final int MSG_COUNT = 100_000;

    public static final String HEADER_FORMAT = """
                        
            Топик:%s Повторы:%s
            Полиси:%s mcl:%s dr:%s
            Сообщений:%s Дубли:%s%%""";
    public static final String STATS_FORMAT = "Min:%s Max:%s Avg:%s";
    public static final String LOG_FORMAT = """
            %s
            %s
            ------------------------""";

    record KafkaRecord(String key, String value) {}

    record TestResults(Integer msgCount, Integer percentage, Duration duration, Map<String, String> topicConfig) {}

    static ArrayListMultimap<String, TestResults> testToResultMap = ArrayListMultimap.create();

    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    Admin kafkaClientAdmin = createKafkaClient();

    @AfterAll
    public static void afterAll() {
        logResults();
    }

    @Test
    @DisplayName("Тест для прогрева соединения с kafka")
    @Order(1)
    public void test_0() {
        //given
        String topicName = "topic0";
        long maxCompactionLagMs = 5000L;
        long deleteRetentionMs = 100L;
        int msgCount = 10_000;
        int percentage = 0;

        List<KafkaRecord> messages = generateKafkaMessages(msgCount, percentage);

        Map<String, String> config = TopicConfigBuilder.cleanupPolicy(CLEANUP_POLICY_COMPACT)
                .setMaxCompactionLagMs(maxCompactionLagMs)
                .setDeleteRetentionMs(deleteRetentionMs)
                .build();

        NewTopic topic = TopicBuilder.name(topicName).configs(config).build();
        createTopic(topic);

        //when
        fillTopicAndGetDuration(topicName, messages);
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
        fillStatistics(topicName, msgCount, percentage, duration, config);
    }

    @RepeatedTest(REPEAT_NUM)
    @DisplayName("Компактный топик 5000:100, 0% дублирующих ключей")
    void test_11() {
        //given
        String topicName = "topic1-2";
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
        fillStatistics(topicName, msgCount, percentage, duration, config);
    }

    @RepeatedTest(REPEAT_NUM)
    @DisplayName("Компактный топик 5000:100, 0% дублирующих ключей")
    void test_12() {
        //given
        String topicName = "topic1-3";
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
        fillStatistics(topicName, msgCount, percentage, duration, config);
    }

    @RepeatedTest(REPEAT_NUM)
    @DisplayName("Компактный топик 5000:100, 0% дублирующих ключей")
    void test_13() {
        //given
        String topicName = "topic1-4";
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
        fillStatistics(topicName, msgCount, percentage, duration, config);
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
        fillStatistics(topicName, msgCount, percentage, duration, config);
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
        fillStatistics(topicName, msgCount, percentage, duration, config);
    }

    private Admin createKafkaClient() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        return Admin.create(properties);
    }

    private void createTopic(NewTopic topic) {
        deleteTopic(topic);
        CreateTopicsResult createTopicsResult = kafkaClientAdmin.createTopics(List.of(topic));
        await().atMost(Duration.ofSeconds(3))
                .until(() -> createTopicsResult.all().isDone());
    }

    private void deleteTopic(NewTopic topic) {
        DeleteTopicsResult deleteTopicsResult = kafkaClientAdmin.deleteTopics(List.of(topic.name()));
        await().atMost(Duration.ofSeconds(3))
                .until(() -> deleteTopicsResult.all().isDone());
    }

    private List<KafkaRecord> generateKafkaMessages(int count, double percentage) {
        if (percentage < 0 || percentage > 100) throw new RuntimeException(format("Percentage error: %s", percentage));
        String repeatableKey = "repeatable-key";

        return IntStream.range(0, count)
                .mapToObj(value -> {
                    long rnd = Math.round((Math.random() * 100));
                    if (rnd <= percentage) {
                        return new KafkaRecord(repeatableKey, Integer.toString(value));
                    } else {
                        return new KafkaRecord(UUID.randomUUID().toString(), Integer.toString(value));
                    }
                }).toList();
    }

    private Duration fillTopicAndGetDuration(String topicName, List<KafkaRecord> messages) {
        long before = System.nanoTime();
        messages.forEach(kafkaRecord -> kafkaTemplate.send(topicName, kafkaRecord.key(), kafkaRecord.value()));
        long after = System.nanoTime();
        return Duration.ofNanos(after - before);
    }

    private void fillStatistics(String topicName,
                                int msgCount,
                                int percentage,
                                Duration duration,
                                Map<String, String> topicConfig) {
        testToResultMap.put(topicName, new TestResults(msgCount, percentage, duration, topicConfig));
    }

    private static void logResults() {
        String toLog = testToResultMap.keySet().stream()
                .map(topicName -> {
                    List<TestResults> testResults = testToResultMap.get(topicName);
                    return createLogMsg(topicName, testResults);
                }).collect(Collectors.joining());

        System.out.println("=====================================");
        System.out.println(toLog);
        System.out.println("=====================================");
    }

    private static String createLogMsg(String topicName, List<TestResults> testResults) {
        String header = testResults.stream().findFirst()
                .map(testResult -> {
                    Map<String, String> topicConfig = testResult.topicConfig();
                    String cleanupPolicy = topicConfig.getOrDefault(CLEANUP_POLICY_CONFIG, "default");
                    String mcl = topicConfig.getOrDefault(MAX_COMPACTION_LAG_MS_CONFIG, "default");
                    String dr = topicConfig.getOrDefault(DELETE_RETENTION_MS_CONFIG, "default");
                    Integer count = testResult.msgCount;
                    Integer percentage = testResult.percentage;

                    return format(HEADER_FORMAT,
                                  topicName, REPEAT_NUM, cleanupPolicy, mcl, dr, count, percentage);
                }).orElseThrow();

        LongSummaryStatistics statistics = testResults.stream()
                .map(testResult -> testResult.duration.toMillis())
                .mapToLong(value -> value)
                .summaryStatistics();
        String stats = format(STATS_FORMAT,
                              statistics.getMin(), statistics.getMax(), statistics.getAverage());

        return format(LOG_FORMAT,
                      header, stats);
    }
}
