package com.karnataka.ksl.kafka;

import com.karnataka.ksl.model.UniversalBusinessRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Component
@ConditionalOnBean(KafkaTemplate.class)
@Slf4j
@RequiredArgsConstructor
public class KslEventProducer {

    public static final String TOPIC_SWS_EVENTS  = "ksl.sws.events";
    public static final String TOPIC_DEPT_EVENTS = "ksl.dept.events";
    public static final String TOPIC_DLQ         = "ksl.dlq";

    private final KafkaTemplate<String, KslEvent> kafkaTemplate;

    public String publishSwsChangeEvent(UniversalBusinessRecord record) {

        KslEvent event = KslEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .correlationId(UUID.randomUUID().toString())
                .eventType(KslEvent.EventType.SWS_TO_DEPT)
                .ubid(record.getUbid())
                .sourceSystem("SWS")
                .record(record)
                .issuedAt(Instant.now())
                .build();

        send(TOPIC_SWS_EVENTS, record.getUbid(), event);

        log.info("[KSL-Producer] SWS event sent eventId={} ubid={} correlationId={}",
                event.getEventId(), record.getUbid(), event.getCorrelationId());

        return event.getCorrelationId();
    }

    public String publishDepartmentChangeEvent(UniversalBusinessRecord record,
                                               String deptCode,
                                               String discoveryMethod) {

        KslEvent event = KslEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .correlationId(UUID.randomUUID().toString())
                .eventType(KslEvent.EventType.DEPT_TO_SWS)
                .ubid(record.getUbid())
                .sourceSystem(deptCode)
                .discoveryMethod(discoveryMethod)
                .record(record)
                .issuedAt(Instant.now())
                .build();

        send(TOPIC_DEPT_EVENTS, record.getUbid(), event);

        return event.getCorrelationId();
    }

    public void sendToDlq(KslEvent original, String reason, int retryCount) {

        KslEvent dlq = original.toBuilder()
                .eventId(UUID.randomUUID().toString())
                .failureReason(reason)
                .retryCount(retryCount)
                .issuedAt(Instant.now())
                .build();

        send(TOPIC_DLQ, original.getUbid(), dlq);

        log.warn("[KSL-DLQ] sent eventId={} reason={}", original.getEventId(), reason);
    }

    private void send(String topic, String key, KslEvent event) {
        try {
            CompletableFuture<SendResult<String, KslEvent>> future =
                    kafkaTemplate.send(topic, key, event);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("[KSL-Producer] Kafka send failed topic={}", topic, ex);
                } else {
                    log.debug("[KSL-Producer] Sent topic={} partition={} offset={}",
                            topic,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });

        } catch (Exception e) {
            log.error("[KSL-Producer] Unexpected failure", e);
        }
    }

    // ---------------- EVENT ----------------

    @lombok.Data
    @lombok.Builder(toBuilder = true)
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class KslEvent {
        private String eventId;
        private String correlationId;
        private EventType eventType;
        private String ubid;
        private String sourceSystem;
        private String discoveryMethod;
        private UniversalBusinessRecord record;
        private Instant issuedAt;
        private String failureReason;
        private int retryCount;

        public enum EventType {
            SWS_TO_DEPT,
            DEPT_TO_SWS,
            CONFLICT_DETECTED,
            RECONCILIATION
        }
    }
}