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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Used for detecting if a delivery queue has continuous failure and should terminate.
 */
public class ContinuousFailureDetector {
    /**
     * The current count of continuous failures.
     */
    protected AtomicLong continuousFailureCount = new AtomicLong(0);

    /**
     * Records when a task has completed.
     *
     * @param success   Whether the task succeeded or not.
     */
    public void didComplete(boolean success) {
        if (success) {
            continuousFailureCount.set(0);
        } else {
            continuousFailureCount.incrementAndGet();
        }
    }

    /**
     * Returns true if the count of continuous failures is equal to or greater than the given threshold.
     *
     * @param threshold The threshold to measure the continuous failure count against.
     * @return          True if the count of continuous failures is equal to or greater than the given threshold.
     */
    public boolean hasFailedContinuously(long threshold) {
        return this.continuousFailureCount.get() >= threshold;
    }
}

