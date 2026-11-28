package org.example.temporal;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;

@Component
@ConditionalOnProperty(name = "temporal.enabled", havingValue = "true")
public class TemporalIdempotencyStore {

    private final RedisTemplate<String, String> redisTemplate;

    public TemporalIdempotencyStore(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public Reservation reserve(String ownerId, String idempotencyKey,
                               String canonicalPayload) {
        validate(ownerId, idempotencyKey);
        String payloadHash = sha256(canonicalPayload);
        String redisKey = "temporal:idempotency:" + operationId(ownerId, idempotencyKey);
        for (int attempt = 0; attempt < 3; attempt++) {
            String existing = redisTemplate.opsForValue().get(redisKey);
            if (existing != null) {
                return existing.equals(payloadHash)
                        ? Reservation.reuse(payloadHash)
                        : Reservation.conflict(payloadHash);
            }

            Boolean inserted = redisTemplate.opsForValue()
                    .setIfAbsent(redisKey, payloadHash, Duration.ofDays(7));
            if (Boolean.TRUE.equals(inserted)) {
                return Reservation.created(payloadHash);
            }
            if (inserted == null) {
                throw new IllegalStateException("Redis did not confirm the idempotency reservation");
            }
        }
        throw new IllegalStateException("Could not resolve a concurrent idempotency reservation");
    }

    public static String operationId(String ownerId, String idempotencyKey) {
        return sha256(ownerId + "\u0000" + idempotencyKey);
    }

    private void validate(String ownerId, String idempotencyKey) {
        if (ownerId == null || ownerId.isBlank()) {
            throw new IllegalArgumentException("User identity is required");
        }
        if (idempotencyKey == null || !idempotencyKey.matches("[A-Za-z0-9._~-]{1,128}")) {
            throw new IllegalArgumentException("Idempotency-Key must be 1-128 safe characters");
        }
    }

    private static String sha256(String payload) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256")
                            .digest(payload.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException("Cannot hash idempotency payload", e);
        }
    }

    public record Reservation(Kind kind, String payloadHash) {
        public enum Kind { CREATED, REUSED, CONFLICT }

        static Reservation created(String hash) { return new Reservation(Kind.CREATED, hash); }
        static Reservation reuse(String hash) { return new Reservation(Kind.REUSED, hash); }
        static Reservation conflict(String hash) { return new Reservation(Kind.CONFLICT, hash); }
    }
}
