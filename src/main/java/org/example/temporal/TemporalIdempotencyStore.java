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

    public synchronized Reservation reserve(String ownerId, String idempotencyKey,
                                            String canonicalPayload) {
        String payloadHash = sha256(canonicalPayload);
        String key = "temporal:idempotency:" + ownerId + ":" + idempotencyKey;
        String existing = redisTemplate.opsForValue().get(key);
        if (existing != null) {
            if (existing.equals(payloadHash)) {
                return Reservation.reuse(payloadHash);
            }
            return Reservation.conflict(payloadHash);
        }
        Boolean set = redisTemplate.opsForValue()
                .setIfAbsent(key, payloadHash, Duration.ofDays(7));
        if (Boolean.FALSE.equals(set)) {
            existing = redisTemplate.opsForValue().get(key);
            if (existing != null && existing.equals(payloadHash)) {
                return Reservation.reuse(payloadHash);
            }
            return Reservation.conflict(payloadHash);
        }
        return Reservation.created(payloadHash);
    }

    private String sha256(String payload) {
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
