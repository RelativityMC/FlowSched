package com.ishland.flowsched.executor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class ExecutorManagerTest {

    private ExecutorManager manager;

    @BeforeEach
    void setUp() {
        manager = new ExecutorManager(4);
    }

    @Test
    void testSimpleSubmit() {
        Task task = mock();

        when(task.priority()).thenReturn(3);
        when(task.lockTokens()).thenReturn(new LockToken[0]);
        final CompletableFuture<Void> future = new CompletableFuture<>();
        doAnswer(invocation -> {
            future.complete(null);
            return null;
        }).when(task).run(any());
        manager.schedule(task);
        future.orTimeout(30, TimeUnit.SECONDS).join();
        verify(task, atLeastOnce()).priority();
        verify(task, atLeastOnce()).lockTokens();
        verify(task, times(1)).run(any());
        verify(task, never()).propagateException(any());
        verifyNoMoreInteractions(task);
    }

    @Test
    void testSimpleSubmitException() {
        Task task = mock();

        when(task.priority()).thenReturn(3);
        when(task.lockTokens()).thenReturn(new LockToken[0]);
        final CompletableFuture<Void> future = new CompletableFuture<>();
        doAnswer(invocation -> {
            future.complete(null);
            throw new RuntimeException("Test exception");
        }).when(task).run(any());
        manager.schedule(task);
        future.orTimeout(30, TimeUnit.SECONDS).join();
        verify(task, atLeastOnce()).priority();
        verify(task, atLeastOnce()).lockTokens();
        verify(task, times(1)).run(any());
        verify(task, times(1)).propagateException(any());
        verifyNoMoreInteractions(task);
    }

    @AfterEach
    void tearDown() {
        manager.shutdown();
        validateMockitoUsage();
    }

}