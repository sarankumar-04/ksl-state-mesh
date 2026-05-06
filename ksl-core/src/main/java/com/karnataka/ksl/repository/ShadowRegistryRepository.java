package com.karnataka.ksl.repository;

import com.karnataka.ksl.model.ShadowRegistryEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public interface ShadowRegistryRepository extends JpaRepository<ShadowRegistryEntry, String> {

    List<ShadowRegistryEntry> findByUbid(String ubid);

    Optional<ShadowRegistryEntry> findByUbidAndDepartmentCode(String ubid, String departmentCode);

    List<ShadowRegistryEntry> findByDepartmentCodeInAndSyncStatus(
        List<String> departmentCodes, ShadowRegistryEntry.SyncStatus syncStatus);

    long countBySyncStatus(ShadowRegistryEntry.SyncStatus syncStatus);

    @Modifying
    @Transactional
    @Query("UPDATE ShadowRegistryEntry s SET s.lastKnownHash = :hash, s.lastSyncedAt = :syncedAt WHERE s.id = :id")
    void updateHash(String id, String hash, Instant syncedAt);
}
