package org.example.temporal;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

@Configuration
@ConditionalOnProperty(name = "temporal.enabled", havingValue = "true")
public class TemporalWorkerConfig {

    private WorkflowServiceStubs service;
    private WorkerFactory workerFactory;

    @Bean
    public WorkerFactory temporalWorkerFactory(
            CreateProgramActivitiesImpl activities,
            @Value("${temporal.address:127.0.0.1:7233}") String temporalAddress) {
        service = WorkflowServiceStubs.newServiceStubs(
                WorkflowServiceStubsOptions.newBuilder()
                        .setTarget(temporalAddress)
                        .build());
        WorkflowClient client = WorkflowClient.newInstance(service);
        workerFactory = WorkerFactory.newInstance(client);
        Worker worker = workerFactory.newWorker(CreateProgramWorkflowImpl.TASK_QUEUE);
        worker.registerWorkflowImplementationTypes(CreateProgramWorkflowImpl.class);
        worker.registerActivitiesImplementations(activities);
        workerFactory.start();
        return workerFactory;
    }

    @Bean
    @DependsOn("temporalWorkerFactory")
    public WorkflowClient temporalWorkflowClient() {
        if (service == null) {
            throw new IllegalStateException("Temporal Worker must be created before WorkflowClient");
        }
        return WorkflowClient.newInstance(service);
    }

    @PreDestroy
    public void shutdown() {
        if (workerFactory != null) {
            workerFactory.shutdown();
        }
        if (service != null) {
            service.shutdown();
        }
    }
}
