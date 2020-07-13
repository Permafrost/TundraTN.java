/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Lachlan Dowding
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package permafrost.tundra.tn.delivery;

import permafrost.tundra.lang.Startable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Used for detecting if a delivery queue has continuous failure and should terminate.
 */
public class ContinuousFailureDetector implements Startable {
    /**
     * The default maximum possible backoff wait.
     */
    private static final long DEFAULT_BACKOFF_WAIT_MAXIMUM_MILLISECONDS = 5L * 60L * 1000L;
    /**
     * The default amount the backoff wait time is incremented by on every task try.
     */
    private static final long DEFAULT_BACKOFF_WAIT_INCREMENT_MILLISECONDS = 10 * 1000L;
    /**
     * The lock used by this detector to serialize threads when backing off processing.
     */
    private final Object backoffSynchronizationLock = new Object();
    /**
     * The lock used by this detector to for waiting threads that are backing off processing.
     */
    private final Object backoffWaitLock = new Object();
    /**
     * The total count of processed tasks.
     */
    protected final AtomicLong totalSuccessCount = new AtomicLong(0);
    /**
     * The total count of failed tasks.
     */
    protected final AtomicLong totalFailureCount = new AtomicLong(0);
    /**
     * The current count of continuous failures.
     */
    protected final AtomicLong currentContinuousFailureCount = new AtomicLong(0);
    /**
     * The current count of threads being backed off.
     */
    protected volatile long currentBackingOffCount = 0;
    /**
     * The continuous failure threshold.
     */
    protected final long threshold;
    /**
     * The maximum time in milliseconds to wait when backing off.
     */
    protected final long maxBackoffWaitTime;
    /**
     * Whether this detector is started.
     */
    protected volatile boolean started = false;

    /**
     * Creates a new ContinuousFailureDetector.
     *
     * @param threshold             The continuous failure threshold.
     * @param maxBackoffWaitTime    The maximum time in milliseconds to wait when backing off due to continuous failure.
     */
    public ContinuousFailureDetector(long threshold, long maxBackoffWaitTime) {
        this.threshold = threshold;
        this.maxBackoffWaitTime = maxBackoffWaitTime <= 0 ? DEFAULT_BACKOFF_WAIT_MAXIMUM_MILLISECONDS : maxBackoffWaitTime;
    }

    /**
     * Records when a task has completed.
     *
     * @param success   Whether the task succeeded or not.
     */
    public void didComplete(boolean success) {
        if (started) {
            if (success) {
                currentContinuousFailureCount.set(0);
                synchronized(backoffWaitLock) {
                    currentBackingOffCount = 0;
                    backoffWaitLock.notifyAll();
                }
                totalSuccessCount.incrementAndGet();
            } else {
                currentContinuousFailureCount.incrementAndGet();
                totalFailureCount.incrementAndGet();
            }
        }
    }

    /**
     * Returns the total number of successful tasks.
     *
     * @return The total number of successful tasks.
     */
    public long getTotalSuccessCount() {
        return totalSuccessCount.get();
    }

    /**
     * Returns the total number failed tasks.
     *
     * @return The total number failed tasks.
     */
    public long getTotalFailureCount() {
        return totalFailureCount.get();
    }

    /**
     * Returns the current number tasks backing off.
     *
     * @return The current number tasks backing off.
     */
    public long getCurrentBackingOffCount() {
        return currentBackingOffCount;
    }

    /**
     * Returns the current number of continuous tasks that have failed.
     *
     * @return The current number of continuous tasks that have failed.
     */
    public long getCurrentContinuousFailureCount() {
        return currentContinuousFailureCount.get();
    }

    /**
     * Returns true if this detector is started.
     *
     * @return true if this detector is started.
     */
    @Override
    public boolean isStarted() {
        return started;
    }

    /**
     * Starts this detector.
     */
    @Override
    public synchronized void start() {
        started = true;
    }

    /**
     * Stops this detector.
     */
    @Override
    public synchronized void stop() {
        if (started) {
            started = false;
            synchronized(backoffWaitLock) {
                backoffWaitLock.notifyAll();
            }
        }
    }

    /**
     * Restarts this detector.
     */
    @Override
    public synchronized void restart() {
        stop();
        start();
    }

    /**
     * Returns true if the count of continuous failures is greater than the given threshold.
     *
     * @return          True if the count of continuous failures is greater than the given threshold.
     */
    public boolean isBackingOff() {
        return isStarted() && threshold > 0 && currentContinuousFailureCount.get() > threshold;
    }

    /**
     * Sleeps the current thread if continuous failure was detected for a variable time based on the number of errors
     * detected up to 1 minute.
     */
    public void backoffIfRequired() {
        if (isBackingOff()) {
            synchronized(backoffSynchronizationLock) {
                if (isBackingOff()) {
                    synchronized(backoffWaitLock) {
                        try {
                            TimeUnit.MILLISECONDS.timedWait(backoffWaitLock, Math.min(++currentBackingOffCount * DEFAULT_BACKOFF_WAIT_INCREMENT_MILLISECONDS, maxBackoffWaitTime));
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
        }
    }
}

