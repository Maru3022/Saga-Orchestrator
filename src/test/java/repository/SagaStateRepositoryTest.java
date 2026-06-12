package repository;

import org.example.model.SagaState;
import org.example.repository.SagaStateRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class SagaStateRepositoryTest {

    private RedisTemplate<String, SagaState> redisTemplate;
    private ValueOperations<String, SagaState> valueOperations;
    private SagaStateRepository repository;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisTemplate = mock(RedisTemplate.class);
        valueOperations = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        repository = new SagaStateRepository(redisTemplate);
    }

    @Test
    void testSave() {
        SagaState state = new SagaState();
        state.setSagaId("saga-123");

        repository.save(state);
        verify(valueOperations, times(1)).set(eq("saga:state:saga-123"), eq(state), any());
    }

    @Test
    void testFindById() {
        SagaState state = new SagaState();
        state.setSagaId("saga-123");

        when(valueOperations.get("saga:state:saga-123")).thenReturn(state);
        Optional<SagaState> result = repository.findById("saga-123");

        assertTrue(result.isPresent());
        assertEquals("saga-123", result.get().getSagaId());
    }

    @Test
    void testFindAllInProgress() {
        when(redisTemplate.keys("saga:state:*")).thenReturn(Set.of("saga:state:saga-123"));

        SagaState state = new SagaState();
        state.setSagaId("saga-123");
        state.setStatus(SagaState.STATUS_IN_PROGRESS);

        when(valueOperations.get("saga:state:saga-123")).thenReturn(state);
        List<SagaState> result = repository.findAllInProgress();

        assertEquals(1, result.size());
    }

    @Test
    void testDelete() {
        repository.delete("saga-123");
        verify(redisTemplate, times(1)).delete("saga:state:saga-123");
    }
}