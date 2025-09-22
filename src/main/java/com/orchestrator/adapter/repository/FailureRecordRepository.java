package com.orchestrator.adapter.repository;

import com.orchestrator.adapter.model.FailureRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface FailureRecordRepository extends JpaRepository<FailureRecord, Long> {

    @Query("SELECT f FROM FailureRecord f WHERE f.failureType = :type ORDER BY f.createdAt DESC")
    List<FailureRecord> findByFailureType(@Param("type") FailureRecord.FailureType type);

    @Query("SELECT f FROM FailureRecord f WHERE f.retryCount < :maxRetries AND f.failureType = 'RETRYABLE' ORDER BY f.createdAt ASC")
    List<FailureRecord> findRetryableRecords(@Param("maxRetries") int maxRetries);

    @Modifying
    @Query("DELETE FROM FailureRecord f WHERE f.createdAt < :cutoffTime")
    int deleteOldRecords(@Param("cutoffTime") Instant cutoffTime);

    @Query("SELECT COUNT(f) FROM FailureRecord f WHERE f.failureType = :type")
    long countByFailureType(@Param("type") FailureRecord.FailureType type);
}