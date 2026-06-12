package org.example.service.handlers;

import org.example.model.SagaState;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Repository
public class SagaStateRepository {

    private static final String KEY_PREFIX = "saga:state:";
    private static final Duration TTL = Duration.ofDays(7);

    private final RedisTemplate<String, SagaState> redisTemplate;

    public SagaStateRepository(RedisTemplate<String, SagaState> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void save(SagaState state) {
        state.touch();
        redisTemplate.opsForValue().set(KEY_PREFIX + state.getSagaId(), state, TTL);
    }

    public Optional<SagaState> findById(String sagaId) {
        return Optional.ofNullable(redisTemplate.opsForValue().get(KEY_PREFIX + sagaId));
    }

    public List<SagaState> findAllInProgress() {
        Set<String> keys = redisTemplate.keys(KEY_PREFIX + "*");
        List<SagaState> result = new ArrayList<>();
        if (keys == null) {
            return result;
        }
        for (String key : keys) {
            SagaState state = redisTemplate.opsForValue().get(key);
            if (state != null && SagaState.STATUS_IN_PROGRESS.equals(state.getStatus())) {
                result.add(state);
            }
        }
        return result;
    }

    public void delete(String sagaId) {
        redisTemplate.delete(KEY_PREFIX + sagaId);
    }
}