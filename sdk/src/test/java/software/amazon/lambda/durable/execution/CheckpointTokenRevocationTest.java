// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.execution;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import software.amazon.awssdk.services.lambda.model.CheckpointDurableExecutionResponse;
import software.amazon.awssdk.services.lambda.model.CheckpointUpdatedExecutionState;
import software.amazon.awssdk.services.lambda.model.ErrorObject;
import software.amazon.awssdk.services.lambda.model.ExecutionDetails;
import software.amazon.awssdk.services.lambda.model.Operation;
import software.amazon.awssdk.services.lambda.model.OperationAction;
import software.amazon.awssdk.services.lambda.model.OperationStatus;
import software.amazon.awssdk.services.lambda.model.OperationType;
import software.amazon.awssdk.services.lambda.model.OperationUpdate;
import software.amazon.lambda.durable.DurableConfig;
import software.amazon.lambda.durable.DurableContext;
import software.amazon.lambda.durable.TypeToken;
import software.amazon.lambda.durable.client.DurableExecutionClient;
import software.amazon.lambda.durable.context.DurableContextImpl;
import software.amazon.lambda.durable.exception.UnrecoverableDurableExecutionException;
import software.amazon.lambda.durable.model.DurableExecutionInput;
import software.amazon.lambda.durable.model.DurableExecutionOutput;
import software.amazon.lambda.durable.model.ExecutionStatus;
import software.amazon.lambda.durable.plugin.DurableExecutionPlugin;
import software.amazon.lambda.durable.plugin.InvocationStatus;
import software.amazon.lambda.durable.serde.JacksonSerDes;

@Timeout(10)
class CheckpointTokenRevocationTest {
    enum Outcome {
        SUCCESS,
        FAILURE,
        RETRY,
        LARGE_RESULT
    }

    enum OperationKind {
        STEP,
        WAIT,
        CALLBACK,
        MAP,
        PARALLEL
    }

    private final DurableExecutionClient client = mock(DurableExecutionClient.class);
    private final DurableExecutionPlugin plugin = mock(DurableExecutionPlugin.class);

    private DurableConfig.Builder config() {
        return DurableConfig.builder().withDurableExecutionClient(client).withPlugins(plugin);
    }

    private DurableExecutionOutput execute(DurableConfig config, BiFunction<String, DurableContext, String> handler) {
        var input = new DurableExecutionInput(
                "arn:aws:lambda:us-east-1:123456789012:function:test/durable-execution/review/exec",
                "token",
                CheckpointUpdatedExecutionState.builder()
                        .operations(Operation.builder()
                                .id("exec")
                                .type(OperationType.EXECUTION)
                                .status(OperationStatus.STARTED)
                                .startTimestamp(Instant.EPOCH)
                                .executionDetails(ExecutionDetails.builder()
                                        .inputPayload("\"input\"")
                                        .build())
                                .build())
                        .build());
        return DurableExecutor.execute(input, null, TypeToken.get(String.class), handler, config);
    }

    @ParameterizedTest
    @EnumSource(Outcome.class)
    void queuedCheckpointRevocationOverridesEveryHandlerOutcome(Outcome outcome) {
        when(client.checkpoint(any(), any(), any()))
                .thenReturn(CheckpointDurableExecutionResponse.builder().build());
        var output = execute(config().withCheckpointDelay(Duration.ofHours(1)).build(), (value, ctx) -> {
            ((DurableContextImpl) ctx)
                    .getExecutionManager()
                    .sendOperationUpdate(OperationUpdate.builder()
                            .id("queued")
                            .type(OperationType.STEP)
                            .action(OperationAction.START)
                            .build());
            return switch (outcome) {
                case SUCCESS -> "done";
                case LARGE_RESULT -> "x".repeat(6 * 1024 * 1024);
                case FAILURE -> throw new IllegalArgumentException("handler failed");
                case RETRY ->
                    throw new UnrecoverableDurableExecutionException(
                            ErrorObject.builder().errorMessage("retry").build(), true);
            };
        });
        assertEquals(ExecutionStatus.PENDING, output.status());
        assertNull(output.result());
        assertNull(output.error());
        verify(client, times(1)).checkpoint(any(), any(), any());
        verify(plugin, times(1))
                .onInvocationEnd(argThat(
                        info -> info.invocationStatus() == InvocationStatus.PENDING && info.executionError() == null));
    }

    @ParameterizedTest
    @EnumSource(OperationKind.class)
    void suspendsThroughPublicOperations(OperationKind kind) {
        when(client.checkpoint(any(), any(), any())).thenAnswer(call -> {
            List<OperationUpdate> updates = call.getArgument(2);
            var revoke = updates.stream().anyMatch(update -> "revoke".equals(update.name()));
            return CheckpointDurableExecutionResponse.builder()
                    .checkpointToken(revoke ? null : "next")
                    .build();
        });
        var output = execute(config().build(), (value, ctx) -> {
            switch (kind) {
                case STEP -> ctx.step("revoke", String.class, stepCtx -> "done");
                case WAIT -> ctx.wait("revoke", Duration.ofSeconds(1));
                case CALLBACK -> ctx.createCallback("revoke", String.class);
                case MAP ->
                    ctx.map(
                            "map",
                            List.of("item"),
                            String.class,
                            (item, index, child) -> child.step("revoke", String.class, stepCtx -> "done"));
                case PARALLEL -> {
                    try (var parallel = ctx.parallel("parallel")) {
                        parallel.branch(
                                "branch", String.class, child -> child.step("revoke", String.class, stepCtx -> "done"));
                        parallel.get();
                    }
                }
            }
            return "unreachable";
        });
        assertEquals(ExecutionStatus.PENDING, output.status());
        verify(client, never()).getExecutionState(any(), any(), any());
    }

    @Test
    void terminalCheckpointWithoutTokenSucceedsWithoutPagination() {
        when(client.checkpoint(any(), any(), any()))
                .thenReturn(CheckpointDurableExecutionResponse.builder()
                        .newExecutionState(CheckpointUpdatedExecutionState.builder()
                                .nextMarker("unused")
                                .build())
                        .build());
        var output = execute(config().build(), (value, ctx) -> "x".repeat(6 * 1024 * 1024));
        assertEquals(ExecutionStatus.SUCCEEDED, output.status());
        assertEquals("", output.result());
        verify(client, times(1))
                .checkpoint(
                        any(),
                        any(),
                        argThat(updates -> updates.size() == 1 && updates.get(0).type() == OperationType.EXECUTION));
        verify(client, never()).getExecutionState(any(), any(), any());
        verify(plugin, times(1))
                .onInvocationEnd(argThat(info -> info.invocationStatus() == InvocationStatus.SUCCEEDED));
    }

    @Test
    void queuedStepDoesNotStartAfterRevocation() {
        var userExecutor = Executors.newSingleThreadExecutor();
        try {
            when(client.checkpoint(any(), any(), any()))
                    .thenReturn(CheckpointDurableExecutionResponse.builder().build());
            var ran = new AtomicBoolean();
            var output = execute(config().withExecutorService(userExecutor).build(), (value, ctx) -> {
                ctx.stepAsync("queued", String.class, stepCtx -> {
                    ran.set(true);
                    return "done";
                });
                ctx.wait("revoke", Duration.ofSeconds(1));
                return "unreachable";
            });
            assertEquals(ExecutionStatus.PENDING, output.status());
            assertFalse(ran.get());
            verify(client, times(1)).checkpoint(any(), any(), any());
        } finally {
            userExecutor.shutdownNow();
        }
    }

    @Test
    void normalDrainAllowsQueuedStepsOnSingleThreadExecutor() {
        var userExecutor = Executors.newSingleThreadExecutor();
        try {
            when(client.checkpoint(any(), any(), any()))
                    .thenReturn(CheckpointDurableExecutionResponse.builder()
                            .checkpointToken("next")
                            .build());
            var ran = new AtomicBoolean();
            var output = execute(config().withExecutorService(userExecutor).build(), (value, ctx) -> {
                ctx.stepAsync("queued", String.class, stepCtx -> {
                    ran.set(true);
                    return "done";
                });
                return "done";
            });
            assertEquals(ExecutionStatus.SUCCEEDED, output.status());
            assertTrue(ran.get());
        } finally {
            userExecutor.shutdownNow();
        }
    }

    @Test
    void runningStepFinishesBeforePendingIsReported() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var revoked = new CountDownLatch(1);
        var finished = new AtomicBoolean();
        when(client.checkpoint(any(), any(), any())).thenAnswer(call -> {
            assertTrue(started.await(5, TimeUnit.SECONDS));
            revoked.countDown();
            return CheckpointDurableExecutionResponse.builder().build();
        });
        var caller = Executors.newSingleThreadExecutor();
        try {
            var future = caller.submit(() -> execute(
                    config().build(),
                    (value, ctx) -> ctx.step("running", String.class, stepCtx -> {
                        started.countDown();
                        await(release);
                        finished.set(true);
                        return "done";
                    })));
            assertTrue(revoked.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> future.get(100, TimeUnit.MILLISECONDS));
            release.countDown();
            assertEquals(
                    ExecutionStatus.PENDING, future.get(5, TimeUnit.SECONDS).status());
            assertTrue(finished.get());
        } finally {
            release.countDown();
            caller.shutdownNow();
        }
    }

    @Test
    void noPollingCanRevokeTokenDuringOutputSerialization() throws Exception {
        var serializing = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var poll = new CountDownLatch(1);
        when(client.checkpoint(any(), any(), any())).thenAnswer(call -> {
            List<OperationUpdate> updates = call.getArgument(2);
            if (updates.isEmpty()) {
                poll.countDown();
                return CheckpointDurableExecutionResponse.builder().build();
            }
            return CheckpointDurableExecutionResponse.builder()
                    .checkpointToken("next")
                    .build();
        });
        var serDes = new JacksonSerDes() {
            @Override
            public String serialize(Object value) {
                serializing.countDown();
                await(release);
                return super.serialize(value);
            }
        };
        var caller = Executors.newSingleThreadExecutor();
        try {
            var future = caller.submit(() -> execute(config().withSerDes(serDes).build(), (value, ctx) -> {
                ctx.waitAsync("unawaited", Duration.ofSeconds(1));
                return "done";
            }));
            assertTrue(serializing.await(5, TimeUnit.SECONDS));
            assertFalse(poll.await(1500, TimeUnit.MILLISECONDS), "Polling must stop before serializing the result");
            release.countDown();
            assertEquals(
                    ExecutionStatus.SUCCEEDED, future.get(5, TimeUnit.SECONDS).status());
            verify(client, times(1)).checkpoint(any(), any(), any());
        } finally {
            release.countDown();
            caller.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}
