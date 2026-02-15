package com.ramya.s3upload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;

/**
 * Wraps an ExecutorService to provide clean result iteration and abort handling.
 * If any submitted task throws an exception, all remaining tasks are cancelled
 * via {@link ExecutorService#shutdownNow()}.
 *
 * @param <V> the result type of the submitted callables
 */
public class ExecutorServiceResultsHandler<V> implements Iterable<V> {

    private static final Logger log = LoggerFactory.getLogger(ExecutorServiceResultsHandler.class);

    private final ExecutorService executorService;
    private final CompletionService<V> completionService;
    private int taskCount = 0;

    public ExecutorServiceResultsHandler(ExecutorService executorService) {
        this.executorService = executorService;
        this.completionService = new ExecutorCompletionService<>(executorService);
    }

    public void submit(Callable<V> task) {
        completionService.submit(task);
        taskCount++;
    }

    public void finishedSubmitting() {
        executorService.shutdown();
    }

    /**
     * Blocks until all submitted tasks have completed. If any task threw an
     * exception, it is propagated after aborting remaining tasks.
     */
    public void awaitCompletion() {
        for (V ignored : this) {
            // just drain results
        }
    }

    /**
     * Aborts all running/pending tasks by calling shutdownNow on the executor.
     */
    public void abort() {
        executorService.shutdownNow();
    }

    @Override
    public Iterator<V> iterator() {
        return new Iterator<V>() {
            private int retrieved = 0;

            @Override
            public boolean hasNext() {
                return retrieved < taskCount;
            }

            @Override
            public V next() {
                try {
                    V result = completionService.take().get();
                    retrieved++;
                    return result;
                } catch (ExecutionException e) {
                    log.error("Task failed, aborting remaining tasks", e.getCause());
                    abort();
                    throw new RuntimeException("Upload task failed: " + e.getCause().getMessage(),
                            e.getCause());
                } catch (InterruptedException e) {
                    abort();
                    throw Utils.runtimeInterruptedException(e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
