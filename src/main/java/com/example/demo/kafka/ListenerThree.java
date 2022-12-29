package com.example.demo.kafka;

import com.example.demo.dto.KafkaMessageDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;

import static java.lang.String.format;
import static org.springframework.kafka.support.KafkaHeaders.RECEIVED_KEY;
import static org.springframework.kafka.support.KafkaHeaders.RECEIVED_TIMESTAMP;
import static org.springframework.kafka.support.KafkaHeaders.RECEIVED_TOPIC;

@Service
@KafkaListener(topics = {"topic-3"}, errorHandler = "kafkaErrorHandler")
@Slf4j
public class ListenerThree implements ConsumerSeekAware {

    @Value("${is-reset-kafka-read-offset-on-startup}")
    Boolean isNeedResetKafkaReadOffset;

    @KafkaHandler
    public void KafkaMessageHandler(@Payload KafkaMessageDto dto,
                                    @Header(RECEIVED_KEY) String key,
                                    @Header(RECEIVED_TOPIC) String topicName,
                                    @Header(RECEIVED_TIMESTAMP) Instant instant) {
        throw new RuntimeException("Просто какая-то ошибка при обработке");
    }

    //Необходимо для откатывания оффсета в топике каждый раз при подключении, что б не заполнять очередь заново
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        if (isNeedResetKafkaReadOffset) {
            assignments.forEach((t, o) -> callback.seekToBeginning(t.topic(), t.partition()));
        }
    }
}
