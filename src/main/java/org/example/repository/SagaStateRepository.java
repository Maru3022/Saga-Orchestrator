package org.example.repository;

import org.example.model.SagaState;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class SagaStateRepository {

    private static final String KEY_PREFIX = "saga:state:";
    private final RedisTemplate<String, SagaState> redisTemplate;

    public SagaStateRepository(RedisTemplate<String, SagaState> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void save(SagaState state) {
        redisTemplate.opsForValue().set(KEY_PREFIX + state.getSagaId(), state);
    }

    public Optional<SagaState> findById(String sagaId) {
        return Optional.ofNullable(redisTemplate.opsForValue().get(KEY_PREFIX + sagaId));
    }

    public void delete(String sagaId) {
        redisTemplate.delete(KEY_PREFIX + sagaId);
    }
}
