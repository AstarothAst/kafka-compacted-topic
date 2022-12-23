package com.example.demo;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.shaded.com.google.common.collect.ArrayListMultimap;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
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
public abstract class AbstractKafkaTest extends AbstractIntegrationTest {

    protected static final int REPEAT_NUM = 3;
    protected static final int MSG_COUNT = 1000;

    public static final String HEADER_FORMAT = """
                        
            Топик:%s Повторы:%s
            Полиси:%s mcl:%s dr:%s
            Сообщений:%s Дубли:%s%%""";
    public static final String STATS_FORMAT = "Min:%s Max:%s Avg:%s";
    public static final String LOG_FORMAT = """
            %s
            %s
            ------------------------""";

    public record KafkaRecord(String key, String value) {}

    public record TestResults(Integer msgCount, Integer percentage, Duration duration, Map<String, String> topicConfig) {}

    @Autowired
    protected KafkaTemplate<String, String> kafkaTemplate;
    Admin kafkaClientAdmin = createKafkaClient();

    @RepeatedTest(REPEAT_NUM)
    @Order(1)
    @DisplayName("Фейковый тест для прогрева соединения с кафкой - без него сильно искажаются в большую сторону результаты идущего первым теста")
    void test_0() {
        //given
        String topicName = "topic0";
        int percentage = 0;

        List<KafkaRecord> messages = generateKafkaMessages(MSG_COUNT, percentage);

        Map<String, String> config = TopicConfigBuilder.cleanupPolicy(CLEANUP_POLICY_COMPACT).build();

        NewTopic topic = TopicBuilder.name(topicName).configs(config).build();
        createTopic(topic);

        //when
        fillTopicAndGetDuration(topicName, messages);
    }

    public Admin createKafkaClient() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        return Admin.create(properties);
    }

    public void createTopic(NewTopic topic) {
        deleteTopic(topic);
        CreateTopicsResult createTopicsResult = kafkaClientAdmin.createTopics(List.of(topic));
        await().atMost(Duration.ofSeconds(3))
                .until(() -> createTopicsResult.all().isDone());
    }

    public void deleteTopic(NewTopic topic) {
        DeleteTopicsResult deleteTopicsResult = kafkaClientAdmin.deleteTopics(List.of(topic.name()));
        await().atMost(Duration.ofSeconds(3))
                .until(() -> deleteTopicsResult.all().isDone());
    }

    public List<KafkaRecord> generateKafkaMessages(int count, double percentage) {
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

    public Duration fillTopicAndGetDuration(String topicName, List<KafkaRecord> messages) {
        long before = System.nanoTime();
        messages.forEach(kafkaRecord -> kafkaTemplate.send(topicName, kafkaRecord.key(), kafkaRecord.value()));
        long after = System.nanoTime();
        return Duration.ofNanos(after - before);
    }

    public void fillStatistics(ArrayListMultimap<String, TestResults> testToResultMap,
                               String topicName,
                               int msgCount,
                               int percentage,
                               Duration duration,
                               Map<String, String> topicConfig) {
        testToResultMap.put(topicName, new TestResults(msgCount, percentage, duration, topicConfig));
    }

    public static void logResults(ArrayListMultimap<String, TestResults> testToResultMap) {
        String toLog = testToResultMap.asMap().entrySet().stream()
                .map(entry -> createLogMsg(entry.getKey(), entry.getValue()))
                .collect(Collectors.joining());

        System.out.println("----------------------------");
        System.out.println(toLog);
    }

    public void sleep(Duration duration) {
        await().pollDelay(duration).until(() -> true);
    }

    private static String createLogMsg(String topicName, Collection<TestResults> testResults) {
        String header = createHeader(topicName, testResults);
        String stats = createStatisticString(testResults);
        return format(LOG_FORMAT, header, stats);
    }

    private static String createHeader(String topicName, Collection<TestResults> testResults) {
        int testCount = testResults.size();
        return testResults.stream()
                .findFirst() // для заполнения хидера подойдет любой элемент, т.к. конфиги топиков у них одинаковые
                .map(testResult -> {
                    Map<String, String> topicConfig = testResult.topicConfig();
                    String cleanupPolicy = topicConfig.getOrDefault(CLEANUP_POLICY_CONFIG, "default");
                    String mcl = topicConfig.getOrDefault(MAX_COMPACTION_LAG_MS_CONFIG, "default");
                    String dr = topicConfig.getOrDefault(DELETE_RETENTION_MS_CONFIG, "default");
                    Integer count = testResult.msgCount;
                    Integer percentage = testResult.percentage;

                    return format(HEADER_FORMAT,
                                  topicName, testCount, cleanupPolicy, mcl, dr, count, percentage);
                }).orElseThrow();
    }

    private static String createStatisticString(Collection<TestResults> testResults) {
        List<Long> allMills = testResults.stream()
                .map(testResult -> testResult.duration.toMillis())
                .toList();

        LongSummaryStatistics statistics = calculateStatistics(allMills);
        return format(STATS_FORMAT,
                      statistics.getMin(), statistics.getMax(), statistics.getAverage());
    }

    private static LongSummaryStatistics calculateStatistics(List<Long> mills) {
        return removeElementsIfNeed(mills).stream()
                .mapToLong(value -> value)
                .summaryStatistics();
    }

    /**
     * Если элементов больше трех, то удаляем самый большой и самый маленький, что бы
     * избежать искажений из-за долгого прогрева соединения, и т.п..
     */
    private static List<Long> removeElementsIfNeed(List<Long> mills) {
        if (mills.size() >= 3) {
            return mills.stream()
                    .sorted(Comparator.reverseOrder())
                    .skip(1)
                    .sorted()
                    .skip(1)
                    .toList();
        } else {
            return mills;
        }
    }
}
