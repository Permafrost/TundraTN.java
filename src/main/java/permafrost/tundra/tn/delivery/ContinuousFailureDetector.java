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

import com.wm.app.b2b.server.ServerThread;
import permafrost.tundra.lang.Startable;
import permafrost.tundra.time.DateTimeHelper;
import permafrost.tundra.time.DurationHelper;
import java.text.MessageFormat;
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
    protected volatile long currentBackoffCount = 0;
    /**
     * The maximum backing off count to calculate a backoff duration for.
     */
    protected final long maxBackoffCount;
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

        long maxBackingOffCount = 0;
        while(calculateRawBackoffWait(maxBackingOffCount) < this.maxBackoffWaitTime) {
            maxBackingOffCount++;
        }
        this.maxBackoffCount = maxBackingOffCount - 1;
    }

    /**
     * Returns the backoff wait in milliseconds for the given backoff count.
     *
     * @param backoffCount  The backoff count.
     * @return              The backoff wait in milliseconds to use for this count.
     */
    private static long calculateRawBackoffWait(long backoffCount) {
        return ((long)Math.pow(2, backoffCount)) * 1000L;
    }

    /**
     * Returns the backoff wait in milliseconds up to the maximum wait for the given backoff count.
     *
     * @param backoffCount  The backoff count.
     * @return              The backoff wait in milliseconds to use for this count.
     */
    protected long calculateBackoffWait(long backoffCount) {
        long backoffWait;
        if (backoffCount > maxBackoffCount) {
            backoffWait = maxBackoffWaitTime;
        } else {
            backoffWait = calculateRawBackoffWait(backoffCount);
        }
        return backoffWait;
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
                    currentBackoffCount = 0;
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
    public long getCurrentBackoffCount() {
        return currentBackoffCount;
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
     * detected up to the configured maximum.
     */
    public void backoffIfRequired() {
        if (isBackingOff()) {
            synchronized(backoffSynchronizationLock) {
                if (isBackingOff()) {
                    synchronized(backoffWaitLock) {
                        Thread currentThread = Thread.currentThread();
                        String currentThreadName = currentThread.getName();
                        try {
                            long backoffWait = calculateBackoffWait(currentBackoffCount);
                            currentThread.setName(MessageFormat.format("{0} (continuous task failure detected, queue processing backing off: continuous failure count = {1}, backoff wait (ms) = {2}, next retry = {3})", currentThreadName, currentBackoffCount, backoffWait, DateTimeHelper.emit(DateTimeHelper.later(DurationHelper.parse(backoffWait)))));
                            TimeUnit.MILLISECONDS.timedWait(backoffWaitLock, backoffWait);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        } finally {
                            currentBackoffCount++;
                            currentThread.setName(currentThreadName);
                            if (currentThread instanceof ServerThread) {
                                // refresh thread start time after each back off wait
                                ((ServerThread)currentThread).setStartTime();
                            }
                        }
                    }
                }
            }
        }
    }
}

