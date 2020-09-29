/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Lachlan Dowding
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

import com.wm.app.b2b.server.InvokeState;
import com.wm.app.b2b.server.ServerAPI;
import permafrost.tundra.lang.StartableManager;
import permafrost.tundra.server.SchedulerHelper;
import permafrost.tundra.server.SchedulerStatus;
import permafrost.tundra.server.ServerThreadFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * DeliveryQueue processing manager.
 */
public class DeliveryQueueManager extends StartableManager<String, DeliveryQueueProcessor> {
    /**
     * How long to wait between each check for delivery queue status changes.
     */
    private static final long DEFAULT_STATUS_CHECK_SCHEDULE_MILLISECONDS = 5 * 1000L;
    /**
     * How often to clean up completed deferred routes from the cache.
     */
    protected static final long DEFAULT_RESTART_SCHEDULE_MILLISECONDS = 60 * 60 * 1000L;
    /**
     * The timeout used when waiting for tasks to complete while shutting down the executor.
     */
    private static final long SCHEDULER_SHUTDOWN_TIMEOUT_MILLISECONDS = 5 * 1000L;

    /**
     * Initialization on demand holder idiom.
     */
    private static class Holder {
        /**
         * The singleton instance of the class.
         */
        private static final DeliveryQueueManager INSTANCE = new DeliveryQueueManager();
    }

    /**
     * Returns the singleton instance of the DeliveryQueue processing manager.
     *
     * @return the singleton instance of the DeliveryQueue processing manager.
     */
    public static DeliveryQueueManager getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * The scheduler used to periodically check for queues being suspended so it can automatically stop the related
     * task processor.
     */
    private ScheduledExecutorService scheduler;

    /**
     * Starts all objects managed by this manager.
     */
    @Override
    public synchronized void start() {
        startup();
        super.start();
    }

    /**
     * Starts up the scheduled supervisor thread.
     */
    private synchronized void startup() {
        scheduler = Executors.newScheduledThreadPool(1, new ServerThreadFactory("TundraTN/Queue Manager", InvokeState.getCurrentState()));
        // shutdown any processors for queues that have been suspended or disabled or if the Integration Server task
        // scheduler has been paused
        scheduler.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                if (REGISTRY.size() > 0) {
                    boolean isSchedulerStarted = SchedulerHelper.status() == SchedulerStatus.STARTED;
                    for (DeliveryQueueProcessor processor : REGISTRY.values()) {
                        try {
                            if (processor.isInvokedByTradingNetworks() && (!isSchedulerStarted || !DeliveryQueueHelper.isProcessing(DeliveryQueueHelper.refresh(processor.getDeliveryQueue())))) {
                                processor.stop();
                            }
                        } catch (Throwable ex) {
                            ServerAPI.logError(ex);
                        }
                    }
                }
            }
        }, DEFAULT_STATUS_CHECK_SCHEDULE_MILLISECONDS, DEFAULT_STATUS_CHECK_SCHEDULE_MILLISECONDS, TimeUnit.MILLISECONDS);

        // restart the scheduler regularly as a safety measure
        scheduler.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                shutdown();
                startup();
            }
        }, DEFAULT_RESTART_SCHEDULE_MILLISECONDS, DEFAULT_RESTART_SCHEDULE_MILLISECONDS, TimeUnit.MILLISECONDS);
    }

    /**
     * Stops all objects managed by this manager.
     */
    @Override
    public synchronized void stop() {
        shutdown();
        super.stop();
    }

    /**
     * Shuts down the scheduled supervisor thread.
     */
    private synchronized void shutdown() {
        try {
            scheduler.shutdown();
            scheduler.awaitTermination(SCHEDULER_SHUTDOWN_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS);
            scheduler.shutdownNow();
        } catch (InterruptedException ex) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
