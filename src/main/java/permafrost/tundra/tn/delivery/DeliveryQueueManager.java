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
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.delivery.DeliveryQueue;
import com.wm.data.IData;
import com.wm.lang.ns.NSName;
import permafrost.tundra.lang.StartableManager;
import permafrost.tundra.server.SchedulerHelper;
import permafrost.tundra.server.SchedulerStatus;
import permafrost.tundra.server.ServerThreadFactory;
import javax.xml.datatype.Duration;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

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
     * How often to refresh the list of queues with pending QUEUED tasks.
     */
    private static final long MINIMUM_PENDING_QUEUE_REFRESH_MILLISECONDS = 1000L;
    /**
     * The cached set of queue names with currently pending QUEUED tasks.
     */
    private final ConcurrentSkipListSet<String> PENDING_QUEUES = new ConcurrentSkipListSet<String>();
    /**
     * The refresh interval in milliseconds for the list of queues with pending QUEUED tasks.
     */
    private volatile long pendingQueuesRefreshInterval = MINIMUM_PENDING_QUEUE_REFRESH_MILLISECONDS;
    /**
     * The system nano time the list of queues with pending QUEUED tasks was last refreshed.
     */
    private volatile long pendingQueuesRefreshTime = 0;
    /**
     * Whether refreshing the list of queues with pending QUEUED tasks is enabled.
     */
    private volatile boolean isPendingQueueRefreshEnabled = false;

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
     * Registers the given queue processor against the queue with the given name.
     *
     * @param queueName         The name of the queue.
     * @param queueProcessor    The processor for the queue with the given name.
     * @return                  If the processor was registered.
     */
    @Override
    public boolean register(String queueName, DeliveryQueueProcessor queueProcessor) {
        return super.register(queueName, queueProcessor);
    }

    /**
     * Unregisters the given queue processor from the queue with the given name.
     *
     * @param queueName         The name of the queue.
     * @param queueProcessor    The processor currently registered for the queue with the given name.
     * @return                  True if the processor was unregistered.
     */
    @Override
    public boolean unregister(String queueName, DeliveryQueueProcessor queueProcessor) {
        return super.unregister(queueName, queueProcessor);
    }

    /**
     * Returns the queue processor registered against the queue with the given name.
     *
     * @param queueName         The name of the queue.
     * @return                  The queue processor registered against the queue with the given name, if any.
     */
    @Override
    public DeliveryQueueProcessor get(String queueName) {
        return super.get(queueName);
    }

    /**
     * Starts all objects managed by this manager.
     */
    @Override
    public synchronized void start() {
        startup();
        super.start();
    }

    /**
     * The regular expression pattern used to match any TundraTN queue delivery services.
     */
    private static final Pattern TUNDRA_DELIVERY_SERVICE_PATTERN = Pattern.compile("tundra\\.tn.*:.*");

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

        // refresh the list of pending queues
        try {
            if (DeliveryQueueHelper.getEnabledQueueCount() > 0 && DeliveryQueueHelper.hasQueuesProcessedByService(TUNDRA_DELIVERY_SERVICE_PATTERN, true)) {
                isPendingQueueRefreshEnabled = true;

                try {
                    long minimumDeliveryInterval = DeliveryQueueHelper.getMinimumDeliveryInterval(TUNDRA_DELIVERY_SERVICE_PATTERN);
                    pendingQueuesRefreshInterval = minimumDeliveryInterval / 2;
                    if (pendingQueuesRefreshInterval < MINIMUM_PENDING_QUEUE_REFRESH_MILLISECONDS) {
                        pendingQueuesRefreshInterval = MINIMUM_PENDING_QUEUE_REFRESH_MILLISECONDS;
                    }
                } catch (Exception ex) {
                    pendingQueuesRefreshInterval = MINIMUM_PENDING_QUEUE_REFRESH_MILLISECONDS;
                }

                scheduler.scheduleWithFixedDelay(new Runnable() {
                    public void run() {
                        refreshPendingQueues();
                    }
                }, 0, pendingQueuesRefreshInterval, TimeUnit.MILLISECONDS);
            }
        } catch(IOException ex) {
            // ignore exception
        } catch(SQLException ex) {
            // ignore exception
        }
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

    /**
     * Refreshes the list of queues with pending QUEUED tasks.
     */
    private void refreshPendingQueues() {
        try {
            Set<String> pendingQueues = DeliveryQueueHelper.getPendingQueues();
            PENDING_QUEUES.addAll(pendingQueues);
            PENDING_QUEUES.retainAll(pendingQueues);
            pendingQueuesRefreshTime = System.nanoTime();
        } catch(SQLException ex) {
            PENDING_QUEUES.clear();
        }
    }

    /**
     * Return whether the given queue has queued tasks.
     *
     * @param queueName The queue to check.
     * @param ordered   Whether delivery queue jobs should be processed in job creation datetime order.
     * @param age       The minimum age a task must be before it is processed.
     * @return          True if the given queue should be processed.
     */
    private boolean hasQueuedTasks(String queueName, boolean ordered, Duration age) throws SQLException {
        boolean hasQueuedTasks = true;
        if (isPendingQueueRefreshEnabled) {
            long millisecondsSinceRefresh = (System.nanoTime() - pendingQueuesRefreshTime) / 1000000L;
            // only check PENDING_QUEUES when not stale
            if (millisecondsSinceRefresh < (pendingQueuesRefreshInterval * 2)) {
                hasQueuedTasks = PENDING_QUEUES.contains(queueName);
            }
        }
        return hasQueuedTasks && ((!ordered && age == null) || DeliveryQueueHelper.size(queueName, ordered, age) > 0);
    }

    /**
     * Returns whether the given queue can be processed.
     *
     * @param queueName The queue to check.
     * @return          True if the given queue should be processed.
     */
    private boolean isNotCurrentlyProcessing(String queueName) {
        return get(queueName) == null;
    }

    /**
     * Return whether the given queue should be processed.
     *
     * @param queueName The queue to check.
     * @param ordered   Whether delivery queue jobs should be processed in job creation datetime order.
     * @param age       The minimum age a task must be before it is processed.
     * @return          True if the given queue should be processed.
     */
    public boolean shouldProcess(String queueName, boolean ordered, Duration age) throws SQLException {
        return isNotCurrentlyProcessing(queueName) && hasQueuedTasks(queueName, ordered, age);
    }

    /**
     * Dequeues each task on the given Trading Networks delivery queue, and processes the task using the given service
     * and input pipeline; if concurrency greater than 1, tasks will be processed by a thread pool whose size is equal
     * to the desired concurrency, otherwise they will be processed on the current thread.
     *
     * @param queueName         The name of the delivery queue whose queued jobs are to be processed.
     * @param service           The service to be invoked to process jobs on the given delivery queue.
     * @param pipeline          The input pipeline used when invoking the given service.
     * @param age               The minimum age a task must be before it is processed.
     * @param concurrency       If greater than 1, this is the number of threads used to process jobs simultaneously.
     * @param retryLimit        The number of retries this job should attempt.
     * @param retryFactor       The factor used to extend the time to wait on each retry.
     * @param timeToWait        The time to wait between each retry.
     * @param threadPriority    The thread priority used when processing tasks.
     * @param daemonize         If true, all threads will be marked as daemons and execution will not end until the JVM
     *                          shuts down or the TN queue is disabled/suspended.
     * @param ordered           Whether delivery queue jobs should be processed in job creation datetime order.
     * @param suspend           Whether to suspend the delivery queue on job retry exhaustion.
     * @param exhaustedStatus   The user status set on the bizdoc when all retries are exhausted.
     * @param errorThreshold    How many continuous errors the queue is allowed to encounter before backing off.
     * @throws IOException      If an I/O error is encountered.
     * @throws SQLException     If a database error is encountered.
     * @throws ServiceException If an error is encountered while processing jobs.
     */
    public void process(String queueName, String service, IData pipeline, Duration age, int concurrency, int retryLimit, float retryFactor, Duration timeToWait, int threadPriority, boolean daemonize, boolean ordered, boolean suspend, String exhaustedStatus, long errorThreshold) throws IOException, SQLException, ServiceException {
        if (queueName == null) throw new NullPointerException("queueName must not be null");
        if (service == null) throw new NullPointerException("service must not be null");

        DeliveryQueue queue = DeliveryQueueHelper.get(queueName);
        if (queue == null) throw new ServiceException("Queue '" + queueName + "' does not exist");

        process(queue, NSName.create(service), pipeline, age, concurrency, retryLimit, retryFactor, timeToWait, threadPriority, daemonize, ordered, suspend, exhaustedStatus, errorThreshold);
    }

    /**
     * Dequeues each task on the given Trading Networks delivery queue, and processes the task using the given service
     * and input pipeline; if concurrency greater than 1, tasks will be processed by a thread pool whose size is equal
     * to the desired concurrency, otherwise they will be processed on the current thread.
     *
     * @param queue             The delivery queue whose queued jobs are to be processed.
     * @param service           The service to be invoked to process jobs on the given delivery queue.
     * @param pipeline          The input pipeline used when invoking the given service.
     * @param age               The minimum age a task must be before it is processed.
     * @param concurrency       If greater than 1, this is the number of threads used to process jobs simultaneously.
     * @param retryLimit        The number of retries this job should attempt.
     * @param retryFactor       The factor used to extend the time to wait on each retry.
     * @param timeToWait        The time to wait between each retry.
     * @param threadPriority    The thread priority used when processing tasks.
     * @param daemonize         If true, all threads will be marked as daemons and execution will not end until the JVM
     *                          shuts down or the TN queue is disabled/suspended.
     * @param ordered           Whether delivery queue jobs should be processed in job creation datetime order.
     * @param suspend           Whether to suspend the delivery queue on job retry exhaustion.
     * @param exhaustedStatus   The user status set on the bizdoc when all retries are exhausted.
     * @param errorThreshold    How many continuous errors the queue is allowed to encounter before backing off.
     * @throws ServiceException If an error is encountered while processing jobs.
     * @throws SQLException     If an error is encountered with the database.
     */
    public void process(DeliveryQueue queue, NSName service, IData pipeline, Duration age, int concurrency, int retryLimit, float retryFactor, Duration timeToWait, int threadPriority, boolean daemonize, boolean ordered, boolean suspend, String exhaustedStatus, long errorThreshold) throws ServiceException, SQLException {
        String queueName = queue.getQueueName();
        if (shouldProcess(queueName, ordered, age)) {
            DeliveryQueueProcessor processor = new DeliveryQueueProcessor(queue, service, pipeline, age, concurrency, retryLimit, retryFactor, timeToWait, threadPriority, daemonize, ordered, suspend, exhaustedStatus, errorThreshold);
            // only allow one processor at a time to process a given queue; if a new processor is started while
            // there is an existing processor, the new processor exits immediately
            if (register(queueName, processor)) {
                try {
                    processor.start();
                    processor.process();
                } finally {
                    unregister(queueName, processor);
                    processor.stop();
                }
            }
        }
    }
}
