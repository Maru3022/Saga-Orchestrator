package repository;

import org.example.model.SagaState;
import org.example.repository.SagaStateRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SagaStateRepositoryTest {

    @Mock
    private RedisTemplate<String, SagaState> redisTemplate;

    @Mock
    private ValueOperations<String, SagaState> valueOperations;

    @InjectMocks
    private SagaStateRepository repository;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    void shouldSaveSagaState() {
        SagaState sagaState = new SagaState("TEST", Map.of());
        sagaState.setSagaId("saga-1");

        repository.save(sagaState);

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(valueOperations).set(keyCaptor.capture(), eq(sagaState));
        assertThat(keyCaptor.getValue()).isEqualTo("saga:state:saga-1");
    }

    @Test
    void shouldFindById(){
        SagaState state = new SagaState("TEST", Map.of());
        state.setSagaId("saga-2");
        when(valueOperations.get("saga:state:saga-2")).thenReturn(state);

        SagaState found = repository.findById("saga-2").orElse(null);

        assertThat(found).isNotNull();
        assertThat(found.getSagaId()).isEqualTo("saga-2");
    }

    @Test
    void shouldDeleteSagaState(){
        repository.delete("saga-3");
        verify(redisTemplate).delete(eq("saga:state:saga-3"));
    }

}
