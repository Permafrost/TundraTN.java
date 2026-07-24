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
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.b2b.server.scheduler.ScheduleManager;
import com.wm.app.b2b.server.scheduler.ScheduledTask;
import com.wm.app.tn.delivery.DeliveryQueue;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.lang.ns.NSName;
import permafrost.tundra.data.IDataCursorHelper;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.lang.StartableManager;
import permafrost.tundra.server.ScheduleHelper;
import permafrost.tundra.server.SchedulerHelper;
import permafrost.tundra.server.SchedulerStatus;
import permafrost.tundra.server.ServerLogHelper;
import permafrost.tundra.server.ServerThreadFactory;
import permafrost.tundra.time.DurationHelper;
import javax.xml.datatype.Duration;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
     * How often to restart the manager to reflect any changes made to the delivery queues.
     */
    protected static final long DEFAULT_RESTART_SCHEDULE_MILLISECONDS = 60 * 60 * 1000L;
    /**
     * The timeout used when waiting for tasks to complete while shutting down the executor.
     */
    private static final long SCHEDULER_SHUTDOWN_TIMEOUT_MILLISECONDS = 5 * 1000L;
    /**
     * Minimum allowed interval to refresh the list of queues with pending QUEUED tasks.
     */
    private static final long MINIMUM_PENDING_QUEUE_REFRESH_MILLISECONDS = 1000L;
    /**
     * Maximum allowed interval to refresh the list of queues with pending QUEUED tasks.
     */
    private static final long MAXIMUM_PENDING_QUEUE_REFRESH_MILLISECONDS = 30 * 1000L;
    /**
     * The acceptable level of latency on the pending queue refresh process before considered stale.
     */
    private static final long ALLOWED_PENDING_QUEUES_REFRESH_LATENCY = 1000L;
    /**
     * How long to wait after startup before we start refreshing pending queues.
     */
    private static final long PENDING_QUEUE_REFRESH_START_DELAY = 5 * 1000L;
    /**
     * Used to calculate the pending queue refresh interval when there are no expedited queues as 10% of the minimum
     * queue delivery interval.
     */
    private static final int QUEUE_DELIVERY_INTERVAL_DIVISOR = 10;
    /**
     * How many times to retry startup when an issue occurs.
     */
    private static final int RETRY_LIMIT = 12;
    /**
     * How long to wait in milliseconds between retries.
     */
    private static final long RETRY_WAIT_MILLISECONDS = 5000L;
    /**
     * The cached set of queue names with currently pending QUEUED tasks.
     */
    private final ConcurrentMap<String, Boolean> PENDING_QUEUES = new ConcurrentHashMap<String, Boolean>();
    /**
     * List of queues with expedited execution and their associated scheduled task IDs.
     */
    private volatile Map<String, ExpeditedQueue> expeditedQueues = null;
    /**
     * The refresh interval in milliseconds for the list of queues with pending QUEUED tasks.
     */
    private volatile long pendingQueuesRefreshInterval = MINIMUM_PENDING_QUEUE_REFRESH_MILLISECONDS;
    /**
     * How long until the pending queues data is considered stale.
     */
    private volatile long pendingQueuesRefreshLatency = pendingQueuesRefreshInterval + ALLOWED_PENDING_QUEUES_REFRESH_LATENCY;
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
                try {
                    if (!REGISTRY.isEmpty()) {
                        boolean isSchedulerStarted = SchedulerHelper.status() == SchedulerStatus.STARTED;
                        for(DeliveryQueueProcessor processor : REGISTRY.values()) {
                            try {
                                if (processor.isInvokedByTradingNetworks() && (!isSchedulerStarted || !DeliveryQueueHelper.isProcessing(DeliveryQueueHelper.refresh(processor.getDeliveryQueue())))) {
                                    processor.stop();
                                }
                            } catch(Throwable ex) {
                                ServerLogHelper.log(ex);
                            }
                        }
                    }
                } catch(Throwable ex) {
                    ServerLogHelper.log(ex);
                }
            }
        }, DEFAULT_STATUS_CHECK_SCHEDULE_MILLISECONDS, DEFAULT_STATUS_CHECK_SCHEDULE_MILLISECONDS, TimeUnit.MILLISECONDS);

        // restart the scheduler regularly as a safety measure
        scheduler.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    shutdown();
                    startup();
                } catch(Throwable ex) {
                    ServerLogHelper.log(ex);
                }
            }
        }, DEFAULT_RESTART_SCHEDULE_MILLISECONDS, DEFAULT_RESTART_SCHEDULE_MILLISECONDS, TimeUnit.MILLISECONDS);

        // refresh the list of pending queues and expedite processing for applicable queues
        try {
            if (DeliveryQueueHelper.getEnabledQueueCount() > 0 && DeliveryQueueHelper.hasQueuesProcessedByService(TUNDRA_DELIVERY_SERVICE_PATTERN, true)) {
                isPendingQueueRefreshEnabled = true;
                pendingQueuesRefreshInterval = MINIMUM_PENDING_QUEUE_REFRESH_MILLISECONDS;

                int retries = 0;
                do {
                    try {
                        expeditedQueues = getExpeditedQueues();
                        if (expeditedQueues.isEmpty()) {
                            long adjustedMinimumDeliveryInterval = DeliveryQueueHelper.getMinimumDeliveryInterval(TUNDRA_DELIVERY_SERVICE_PATTERN) / QUEUE_DELIVERY_INTERVAL_DIVISOR;
                            pendingQueuesRefreshInterval = Math.max(Math.min(adjustedMinimumDeliveryInterval, MAXIMUM_PENDING_QUEUE_REFRESH_MILLISECONDS), MINIMUM_PENDING_QUEUE_REFRESH_MILLISECONDS);
                        }
                        break;
                    } catch(Exception ex) {
                        ServerLogHelper.log(ex);
                        Thread.sleep(RETRY_WAIT_MILLISECONDS);
                        retries++;
                    }
                } while(retries < RETRY_LIMIT);

                pendingQueuesRefreshLatency = pendingQueuesRefreshInterval + ALLOWED_PENDING_QUEUES_REFRESH_LATENCY;

                scheduler.scheduleWithFixedDelay(new Runnable() {
                    public void run() {
                        try {
                            if (SchedulerHelper.status() == SchedulerStatus.STARTED) {
                                refreshPendingQueues();
                                expeditePendingQueues();
                            }
                        } catch(Throwable ex) {
                            ServerLogHelper.log(ex);
                        }
                    }
                }, PENDING_QUEUE_REFRESH_START_DELAY, pendingQueuesRefreshInterval, TimeUnit.MILLISECONDS);
            }
        } catch(Exception ex) {
            ServerLogHelper.log(ex);
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
    private synchronized void refreshPendingQueues() {
        long start = System.nanoTime();
        try {
            Map<String, Boolean> pendingQueues = DeliveryQueueHelper.getPendingQueues();
            PENDING_QUEUES.putAll(pendingQueues);
            PENDING_QUEUES.keySet().retainAll(pendingQueues.keySet());
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
            // only check pending queues when not stale
            if (millisecondsSinceRefresh < pendingQueuesRefreshLatency) {
                hasQueuedTasks = PENDING_QUEUES.containsKey(queueName) && (!ordered || PENDING_QUEUES.get(queueName));
            }
        }
        return hasQueuedTasks && (!DurationHelper.isGreaterThanZero(age) || DeliveryQueueHelper.size(queueName, ordered, age) > 0);
    }

    /**
     * Returns whether the given queue is currently being processed.
     *
     * @param queueName The queue to check.
     * @return          True if the given queue is currently being processed.
     */
    private boolean currentlyProcessing(String queueName) {
        return get(queueName) != null;
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
        return !currentlyProcessing(queueName) && hasQueuedTasks(queueName, ordered, age);
    }

    /**
     * Expedites the processing of any expedited pending queues.
     */
    private synchronized void expeditePendingQueues() throws ServiceException {
        // determine list of scheduled tasks to be expedited
        if (!PENDING_QUEUES.isEmpty()) {
            Map<String, ExpeditedQueue> expeditedQueues = this.expeditedQueues;
            if (expeditedQueues != null && !expeditedQueues.isEmpty()) {
                List<String> expeditedTasks = null;
                for(Map.Entry<String, Boolean> entry : PENDING_QUEUES.entrySet()) {
                    String pendingQueueName = entry.getKey();
                    boolean pendingQueueOrdered = entry.getValue();

                    if (expeditedQueues.containsKey(pendingQueueName)) {
                        if (expeditedTasks == null) {
                            expeditedTasks = new ArrayList<String>(Math.min(expeditedQueues.size(), PENDING_QUEUES.size()));
                        }

                        ExpeditedQueue expeditedQueue = expeditedQueues.get(pendingQueueName);
                        String taskID = expeditedQueue.getTask();
                        boolean ordered = expeditedQueue.isOrdered();

                        if ((!ordered || pendingQueueOrdered) && isQueueDeliveryTask(pendingQueueName, taskID)) {
                            expeditedTasks.add(taskID);
                        }
                    }
                }

                // expedite applicable scheduled tasks
                if (expeditedTasks != null && !expeditedTasks.isEmpty()) {
                    ScheduleHelper.expedite(expeditedTasks);
                }
            }
        }
    }

    /**
     * Returns true if scheduled task with the given identity is the delivery task for the given queue name.
     *
     * @param queueName The name of the queue.
     * @param taskID    The scheduled task identity.
     * @return          True if the scheduled task with the given identity is the delivery task for the given queue name.
     */
    private static boolean isQueueDeliveryTask(String queueName, String taskID) {
        boolean isQueueDeliveryTask = false;
        if (queueName != null && taskID != null) {
            try {
                ScheduledTask task = ScheduleManager.getTask(taskID);
                if (task != null) {
                    String service = task.getService();
                    IData pipeline = task.getInputs();
                    if (pipeline != null) {
                        IDataCursor pipelineCursor = pipeline.getCursor();
                        try {
                            isQueueDeliveryTask = DeliveryQueueHelper.DELIVER_BATCH_SERVICE_NAME.equals(service) && queueName.equals(IDataCursorHelper.get(pipelineCursor, String.class, "queue"));
                        } finally {
                            pipelineCursor.destroy();
                        }
                    }
                }
            } catch (SQLException ex) {
                // ignore exception
            }
        }
        return isQueueDeliveryTask;
    }

    /**
     * Helper class to store expedited queue properties.
     */
    private static class ExpeditedQueue {
        private final String name;
        private final String task;
        private final boolean ordered;

        public ExpeditedQueue(String name, String task, boolean ordered) {
            this.name = name;
            this.task = task;
            this.ordered = ordered;
        }

        public String getName() {
            return name;
        }

        public String getTask() {
            return task;
        }

        public boolean isOrdered() {
            return ordered;
        }
    }

    /**
     * Returns all expedited queues and their associated scheduled task IDs.
     *
     * @return                  All expedited queues and their associated scheduled task IDs.
     * @throws ServiceException If an error occurs.
     */
    private static Map<String, ExpeditedQueue> getExpeditedQueues() throws ServiceException {
        Map<String, ExpeditedQueue> expeditedQueues;
        IData[] tasks = ScheduleHelper.list(DeliveryQueueHelper.DELIVER_BATCH_SERVICE_NAME, "%$schedule/pipeline/$expedite?% == \"true\" and %$schedule/pipeline/$daemonize?% != \"true\" and %$schedule/pipeline/$task.age% == $null", null);
        if (tasks != null && tasks.length > 0) {
            expeditedQueues = new HashMap<String, ExpeditedQueue>(tasks.length);
            for (IData task : tasks) {
                if (task != null) {
                    IDataCursor cursor = task.getCursor();
                    try {
                        String taskID = IDataCursorHelper.get(cursor, String.class, "id");
                        if (taskID != null) {
                            IData pipeline = IDataCursorHelper.get(cursor, IData.class, "pipeline");
                            if (pipeline != null) {
                                IDataCursor pipelineCursor = pipeline.getCursor();
                                try {
                                    String queueName = IDataCursorHelper.get(pipelineCursor, String.class, "queue");
                                    if (queueName != null) {
                                        try {
                                            DeliveryQueue queue = DeliveryQueueHelper.get(queueName);
                                            String service = queue.getSchedule().getService();
                                            if (service != null) {
                                                boolean ordered = IDataHelper.getOrDefault(pipelineCursor, "$ordered?", Boolean.class, false);
                                                expeditedQueues.put(queueName, new ExpeditedQueue(queueName, taskID, ordered));
                                            }
                                        } catch(IOException ex) {
                                            ServerLogHelper.log(ex);
                                        } catch(SQLException ex) {
                                            ServerLogHelper.log(ex);
                                        }
                                    }
                                } finally {
                                    pipelineCursor.destroy();
                                }
                            }
                        }
                    } finally {
                        cursor.destroy();
                    }
                }
            }
        } else {
            expeditedQueues = Collections.emptyMap();
        }
        return expeditedQueues;
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
     * @param exhaustedService  Optional service to be invoked when all retries are exhausted.
     * @param errorThreshold    How many continuous errors the queue is allowed to encounter before backing off.
     * @throws IOException      If an I/O error is encountered.
     * @throws SQLException     If a database error is encountered.
     * @throws ServiceException If an error is encountered while processing jobs.
     */
    public void process(String queueName, String service, IData pipeline, Duration age, int concurrency, int retryLimit, float retryFactor, Duration timeToWait, int threadPriority, boolean daemonize, boolean ordered, boolean suspend, String exhaustedStatus, String exhaustedService, long errorThreshold) throws IOException, SQLException, ServiceException {
        if (queueName == null) throw new NullPointerException("queueName must not be null");
        if (service == null) throw new NullPointerException("service must not be null");

        DeliveryQueue queue = DeliveryQueueHelper.get(queueName);
        if (queue == null) throw new ServiceException("Queue '" + queueName + "' does not exist");

        process(queue, NSName.create(service), pipeline, age, concurrency, retryLimit, retryFactor, timeToWait, threadPriority, daemonize, ordered, suspend, exhaustedStatus, exhaustedService == null ? null : NSName.create(exhaustedService), errorThreshold);
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
     * @param exhaustedService  Optional service to be invoked when all retries are exhausted.
     * @param errorThreshold    How many continuous errors the queue is allowed to encounter before backing off.
     * @throws ServiceException If an error is encountered while processing jobs.
     * @throws SQLException     If an error is encountered with the database.
     */
    public void process(DeliveryQueue queue, NSName service, IData pipeline, Duration age, int concurrency, int retryLimit, float retryFactor, Duration timeToWait, int threadPriority, boolean daemonize, boolean ordered, boolean suspend, String exhaustedStatus, NSName exhaustedService, long errorThreshold) throws ServiceException, SQLException {
        String queueName = queue.getQueueName();
        if (shouldProcess(queueName, ordered, age)) {
            DeliveryQueueProcessor processor = new DeliveryQueueProcessor(queue, service, pipeline, age, concurrency, retryLimit, retryFactor, timeToWait, threadPriority, daemonize, ordered, suspend, exhaustedStatus, exhaustedService, errorThreshold);
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
