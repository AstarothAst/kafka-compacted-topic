package com.example.demo;

import org.jetbrains.annotations.NotNull;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.support.TestPropertySourceUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static java.lang.String.format;

@ActiveProfiles("test")
@SpringBootTest()
@ContextConfiguration(initializers = AbstractIntegrationTest.Initializer.class)
@Testcontainers
public abstract class AbstractIntegrationTest extends AbstractTest {

    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"));

    static {
        kafka.start();
    }

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(@NotNull ConfigurableApplicationContext applicationContext) {
            String kafkaConsumerBootstrapServers = format("spring.kafka.consumer.bootstrap-servers=%s", kafka.getBootstrapServers());
            String kafkaProducersBootstrapServers = format("spring.kafka.producer.bootstrap-servers=%s", kafka.getBootstrapServers());
            String kafkaAutoCreateTopics = format("auto.create.topics.enable=%s", true);

            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
                    applicationContext,
                    kafkaConsumerBootstrapServers,
                    kafkaProducersBootstrapServers,
                    kafkaAutoCreateTopics
            );
        }
    }
}