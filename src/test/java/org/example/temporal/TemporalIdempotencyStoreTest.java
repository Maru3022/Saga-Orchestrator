package org.example.temporal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class TemporalIdempotencyStoreTest {

    private RedisTemplate<String, String> redisTemplate;
    private ValueOperations<String, String> valueOperations;
    private TemporalIdempotencyStore store;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisTemplate = mock(RedisTemplate.class);
        valueOperations = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        store = new TemporalIdempotencyStore(redisTemplate);
    }

    @Test
    void atomicallyReservesAnOwnerScopedKey() {
        when(valueOperations.get(anyString())).thenReturn(null);
        when(valueOperations.setIfAbsent(anyString(), anyString(), any(Duration.class)))
                .thenReturn(true);

        var reservation = store.reserve("owner-1", "request-1", "{\"goal\":\"fit\"}");

        assertEquals(TemporalIdempotencyStore.Reservation.Kind.CREATED, reservation.kind());
        verify(valueOperations).setIfAbsent(
                eq("temporal:idempotency:"
                        + TemporalIdempotencyStore.operationId("owner-1", "request-1")),
                anyString(),
                eq(Duration.ofDays(7)));
    }

    @Test
    void reusesMatchingPayloadAndRejectsChangedPayload() throws Exception {
        String matchingHash = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256")
                        .digest("{\"goal\":\"fit\"}".getBytes(StandardCharsets.UTF_8)));
        when(valueOperations.get(anyString())).thenReturn(matchingHash);

        var same = store.reserve("owner-1", "request-1", "{\"goal\":\"fit\"}");
        var changed = store.reserve("owner-1", "request-1", "{\"goal\":\"other\"}");

        assertEquals(TemporalIdempotencyStore.Reservation.Kind.REUSED, same.kind());
        assertEquals(TemporalIdempotencyStore.Reservation.Kind.CONFLICT, changed.kind());
    }

    @Test
    void rejectsMalformedIdempotencyKeysBeforeAccessingRedis() {
        assertThrows(IllegalArgumentException.class,
                () -> store.reserve("owner-1", "bad:key", "{}"));
        verifyNoInteractions(redisTemplate);
    }

    @Test
    void failsIfRedisDoesNotConfirmReservation() {
        when(valueOperations.get(anyString())).thenReturn(null);
        when(valueOperations.setIfAbsent(anyString(), anyString(), any(Duration.class)))
                .thenReturn(null);

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> store.reserve("owner-1", "request-1", "{}"));

        assertTrue(error.getMessage().contains("did not confirm"));
    }
}
