package com.karnataka.ksl.repository;

import com.karnataka.ksl.model.AuditLedgerEntry;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface AuditLedgerRepository extends JpaRepository<AuditLedgerEntry, String> {

    List<AuditLedgerEntry> findByUbidOrderByRecordedAtDesc(String ubid);

    List<AuditLedgerEntry> findByCorrelationIdOrderByRecordedAtAsc(String correlationId);

    List<AuditLedgerEntry> findByPropagationStatus(AuditLedgerEntry.PropagationStatus status);

    // Paginated — used by the dashboard "All Entries" view
    Page<AuditLedgerEntry> findAllByOrderByRecordedAtDesc(Pageable pageable);

    long countByPropagationStatus(AuditLedgerEntry.PropagationStatus status);
}
