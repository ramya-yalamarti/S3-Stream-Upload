package com.ramya.s3upload;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A blocking queue that can be externally closed. When closed, any thread
 * blocked on {@link #put} will wake up and receive an IllegalStateException
 * instead of blocking forever. This prevents thread hangs during abort scenarios.
 *
 * @param <T> the type of elements in the queue
 */
public class ClosableQueue<T> extends ArrayBlockingQueue<T> {

    private volatile boolean closed = false;

    public ClosableQueue(int capacity) {
        super(capacity);
    }

    /**
     * Closes this queue. Any current or future calls to {@link #put} will throw
     * IllegalStateException once they detect the closed state.
     */
    public void close() {
        closed = true;
    }

    /**
     * Places an item on the queue, polling every second to check if the queue
     * has been closed. This avoids indefinite blocking when an error occurs
     * in another thread and the upload is being aborted.
     *
     * @throws InterruptedException  if the thread is interrupted while waiting
     * @throws IllegalStateException if the queue has been closed
     */
    @Override
    public void put(T item) throws InterruptedException {
        while (!offer(item, 1, TimeUnit.SECONDS)) {
            if (closed) {
                throw new IllegalStateException(
                        "Queue has been closed due to an error elsewhere. Upload is aborting.");
            }
        }
    }
}
