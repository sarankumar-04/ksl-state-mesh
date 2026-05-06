package com.karnataka.ksl.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.karnataka.ksl.adapter.DepartmentAdapter;
import com.karnataka.ksl.audit.AuditService;
import com.karnataka.ksl.conflict.ConflictResolutionEngine;
import com.karnataka.ksl.model.AuditLedgerEntry;
import com.karnataka.ksl.model.UniversalBusinessRecord;
import com.karnataka.ksl.repository.ShadowRegistryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * KSL Event Consumer — only active when Kafka is configured.
 * In sandbox mode (no Kafka), this bean is not created.
 */
@Component
@ConditionalOnBean(KafkaTemplate.class)
@Slf4j
@RequiredArgsConstructor
public class KslEventConsumer {

    private final Map<String, DepartmentAdapter> adapters;
    private final ShadowRegistryRepository shadowRegistry;
    private final AuditService auditService;
    private final ConflictResolutionEngine conflictEngine;
    private final KslEventProducer eventProducer;
    private final ObjectMapper objectMapper;

    private final ConcurrentHashMap<String, Boolean> processedEvents = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConflictResolutionEngine.PendingUpdate> inFlightByUbid = new ConcurrentHashMap<>();

    @KafkaListener(topics = KslEventProducer.TOPIC_SWS_EVENTS, groupId = "ksl-sws-consumer",
                   containerFactory = "kslKafkaListenerContainerFactory")
    public void consumeSwsEvent(@Payload String payload,
                                 @Header(KafkaHeaders.RECEIVED_KEY) String ubid,
                                 Acknowledgment ack) {
        KslEventProducer.KslEvent event = deserialize(payload);
        if (event == null) { ack.acknowledge(); return; }
        if (processedEvents.putIfAbsent(event.getEventId(), true) != null) {
            log.info("[Consumer] Duplicate skipped: {}", event.getEventId());
            ack.acknowledge(); return;
        }
        fanOutToDepartments(event);
        ack.acknowledge();
    }

    @KafkaListener(topics = KslEventProducer.TOPIC_DEPT_EVENTS, groupId = "ksl-dept-consumer",
                   containerFactory = "kslKafkaListenerContainerFactory")
    public void consumeDeptEvent(@Payload String payload,
                                  @Header(KafkaHeaders.RECEIVED_KEY) String ubid,
                                  Acknowledgment ack) {
        KslEventProducer.KslEvent event = deserialize(payload);
        if (event == null) { ack.acknowledge(); return; }
        if (processedEvents.putIfAbsent(event.getEventId(), true) != null) {
            ack.acknowledge(); return;
        }
        checkAndHandleConflict(event);
        writeToSws(event);
        ack.acknowledge();
    }

    private void fanOutToDepartments(KslEventProducer.KslEvent event) {
        UniversalBusinessRecord record = event.getRecord();
        List<String> targetDepts = shadowRegistry.findByUbid(record.getUbid()).stream()
            .map(e -> e.getDepartmentCode())
            .filter(code -> !code.equals("SWS"))
            .toList();

        for (String deptCode : targetDepts) {
            DepartmentAdapter adapter = adapters.get(deptCode);
            if (adapter == null) continue;
            String idempotencyKey = event.getCorrelationId() + ":" + deptCode;
            DepartmentAdapter.WriteResult result = adapter.write(record, idempotencyKey);
            auditService.logPropagation(event.getCorrelationId(), record.getUbid(), "SWS", deptCode,
                result.targetRecordId(),
                result.success() ? AuditLedgerEntry.PropagationStatus.SUCCESS : AuditLedgerEntry.PropagationStatus.FAILED,
                result.errorMessage(), null);
        }
    }

    private void writeToSws(KslEventProducer.KslEvent event) {
        DepartmentAdapter swsAdapter = adapters.get("SWS");
        if (swsAdapter == null) return;
        String idempotencyKey = event.getCorrelationId() + ":SWS";
        DepartmentAdapter.WriteResult result = swsAdapter.write(event.getRecord(), idempotencyKey);
        auditService.logPropagation(event.getCorrelationId(), event.getUbid(),
            event.getSourceSystem(), "SWS", result.targetRecordId(),
            result.success() ? AuditLedgerEntry.PropagationStatus.SUCCESS : AuditLedgerEntry.PropagationStatus.FAILED,
            result.errorMessage(), null);
    }

    private void checkAndHandleConflict(KslEventProducer.KslEvent incomingEvent) {
        ConflictResolutionEngine.PendingUpdate incoming = new ConflictResolutionEngine.PendingUpdate(
            incomingEvent.getUbid(), incomingEvent.getSourceSystem(), incomingEvent.getEventId(),
            incomingEvent.getRecord(), null, Instant.now());
        ConflictResolutionEngine.PendingUpdate existing = inFlightByUbid.get(incomingEvent.getUbid());
        if (existing != null && conflictEngine.isConflict(incoming, existing)) {
            ConflictResolutionEngine.ResolutionResult resolution = conflictEngine.resolve(incoming, existing);
            auditService.logConflict(incomingEvent.getCorrelationId(), incomingEvent.getUbid(), resolution);
        } else {
            inFlightByUbid.put(incomingEvent.getUbid(), incoming);
        }
    }

    private KslEventProducer.KslEvent deserialize(String payload) {
        try { return objectMapper.readValue(payload, KslEventProducer.KslEvent.class); }
        catch (Exception e) { log.error("[Consumer] Deserialize failed: {}", e.getMessage()); return null; }
    }
}
