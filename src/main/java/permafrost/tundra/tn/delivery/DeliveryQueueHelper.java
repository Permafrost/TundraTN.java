/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Lachlan Dowding
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
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.b2b.server.Session;
import com.wm.app.b2b.server.scheduler.ScheduledTask;
import com.wm.app.tn.db.Datastore;
import com.wm.app.tn.db.QueueOperations;
import com.wm.app.tn.db.SQLWrappers;
import com.wm.app.tn.delivery.DeliveryQueue;
import com.wm.app.tn.delivery.DeliverySchedule;
import com.wm.app.tn.delivery.GuaranteedJob;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.data.IDataUtil;
import com.wm.lang.ns.NSName;
import com.wm.util.Masks;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.lang.BooleanHelper;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.lang.IdentityHelper;
import permafrost.tundra.lang.StringHelper;
import permafrost.tundra.lang.ThreadHelper;
import permafrost.tundra.server.BlockingServerThreadPoolExecutor;
import permafrost.tundra.time.DateTimeHelper;
import permafrost.tundra.tn.document.BizDocEnvelopeHelper;
import permafrost.tundra.tn.profile.ProfileCache;
import permafrost.tundra.util.concurrent.DirectExecutorService;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * A collection of convenience methods for working with Trading Networks delivery queues.
 */
public final class DeliveryQueueHelper {
    /**
     * SQL statement to select head of a delivery queue in job creation datetime order.
     */
    private static final String SELECT_NEXT_DELIVERY_JOB_ORDERED_SQL = "SELECT JobID FROM DeliveryJob WHERE QueueName = ? AND JobStatus = 'QUEUED' AND TimeCreated = (SELECT MIN(TimeCreated) FROM DeliveryJob WHERE QueueName = ? AND JobStatus = 'QUEUED') AND TimeUpdated <= ?";

    /**
     * SQL statement to select head of a delivery queue in indeterminate order.
     */
    private static final String SELECT_NEXT_DELIVERY_JOB_UNORDERED_SQL = "SELECT JobID FROM DeliveryJob WHERE QueueName = ? AND JobStatus = 'QUEUED' AND TimeCreated = (SELECT MIN(TimeCreated) FROM DeliveryJob WHERE QueueName = ? AND JobStatus = 'QUEUED' AND TimeUpdated <= ?)";

    /**
     * The age a delivery job must be before it is eligible to be processed.
     */
    private static final long DELIVERY_JOB_AGE_THRESHOLD_MILLISECONDS = 750L;

    /**
     * The name of the service that Trading Networks uses to invoke delivery queue processing services.
     */
    private static final String DELIVER_BATCH_SERVICE_NAME = "wm.tn.queuing:deliverBatch";

    /**
     * The name of the service used to update the completion status of a delivery queue job.
     */
    private static final NSName UPDATE_QUEUED_TASK_SERVICE_NAME = NSName.create("wm.tn.queuing:updateQueuedTask");

    /**
     * The name of the service used to update a delivery queue.
     */
    private static final NSName UPDATE_QUEUE_SERVICE_NAME = NSName.create("wm.tn.queuing:updateQueue");

    /**
     * The minimum wait between each poll of a delivery queue for more jobs.
     */
    private static final long MIN_WAIT_BETWEEN_DELIVERY_QUEUE_POLLS_MILLISECONDS = 1L;

    /**
     * The wait between each refresh of a delivery queue settings from the database.
     */
    private static final long WAIT_BETWEEN_DELIVERY_QUEUE_REFRESH_MILLISECONDS = 5L * 1000L;

    /**
     * The suffix used on worker thread names.
     */
    private static final String WORKER_THREAD_SUFFIX = ": Worker";

    /**
     * The suffix used on supervisor thread names.
     */
    private static final String SUPERVISOR_THREAD_SUFFIX = ": Supervisor";

    /**
     * The bizdoc user status to use when a job is dequeued.
     */
    private static final String DEQUEUED_USER_STATUS = "DEQUEUED";

    /**
     * Disallow instantiation of this class.
     */
    private DeliveryQueueHelper() {}

    /**
     * Returns the Trading Networks delivery queue associated with the given name.
     *
     * @param queueName     The name of the queue to return.
     * @return              The delivery queue with the given name.
     * @throws IOException  If an I/O error is encountered.
     * @throws SQLException If a database error is encountered.
     */
    public static DeliveryQueue get(String queueName) throws IOException, SQLException {
        if (queueName == null) return null;
        return QueueOperations.selectByName(queueName);
    }

    /**
     * Refreshes the given Trading Networks delivery queue from the database.
     *
     * @param queue         The queue to be refreshed.
     * @return              The given queue, refreshed from the database.
     * @throws IOException  If an I/O error is encountered.
     * @throws SQLException If a database error is encountered.
     */
    public static DeliveryQueue refresh(DeliveryQueue queue) throws IOException, SQLException {
        return get(queue.getQueueName());
    }

    /**
     * Returns a list of all registered Trading Networks delivery queues.
     *
     * @return              A list of all registered Trading Networks delivery queues.
     * @throws IOException  If an I/O error is encountered.
     * @throws SQLException If a database error is encountered.
     * */
    public static DeliveryQueue[] list() throws IOException, SQLException {
        return QueueOperations.select(null);
    }

    /**
     * Enables the delivery of the given Trading Networks delivery queue.
     *
     * @param queue The queue to enable delivery on.
     * @throws ServiceException If a database error occurs.
     */
    public static void enable(DeliveryQueue queue) throws ServiceException {
        if (queue == null) return;
        queue.setState(DeliveryQueue.STATE_ENABLED);
        save(queue);
    }

    /**
     * Disables the delivery of the given Trading Networks delivery queue.
     *
     * @param queue The queue to enable delivery on.
     * @throws ServiceException If a database error occurs.
     */
    public static void disable(DeliveryQueue queue) throws ServiceException {
        if (queue == null) return;
        queue.setState(DeliveryQueue.STATE_DISABLED);
        save(queue);
    }

    /**
     * Drains the delivery of the given Trading Networks delivery queue.
     *
     * @param queue The queue to enable delivery on.
     * @throws ServiceException If a database error occurs.
     */
    public static void drain(DeliveryQueue queue) throws ServiceException {
        if (queue == null) return;
        queue.setState(DeliveryQueue.STATE_DRAINING);
        save(queue);
    }

    /**
     * Suspends the delivery of the given Trading Networks delivery queue.
     *
     * @param queue The queue to enable delivery on.
     * @throws ServiceException If a database error occurs.
     */
    public static void suspend(DeliveryQueue queue) throws ServiceException {
        if (queue == null) return;
        queue.setState(DeliveryQueue.STATE_SUSPENDED);
        save(queue);
    }

    /**
     * Returns the number of jobs currently queued in the given Trading Networks delivery queue.
     *
     * @param queue The queue to return the length of.
     * @return      The length of the given queue, which is the number of delivery jobs with a status
     *              of QUEUED or DELIVERING.
     * @throws ServiceException If a database error occurs.
     */
    public static int length(DeliveryQueue queue) throws ServiceException {
        int length = 0;

        if (queue != null) {
            try {
                String[] jobs = QueueOperations.getQueuedJobs(queue.getQueueName());
                if (jobs != null) length = jobs.length;
            } catch(SQLException ex) {
                ExceptionHelper.raise(ex);
            }
        }

        return length;
    }

    /**
     * Updates the given Trading Networks delivery queue with any changes that may have occurred.
     *
     * @param queue The queue whose changes are to be saved.
     * @throws ServiceException If a database error occurs.
     */
    public static void save(DeliveryQueue queue) throws ServiceException {
        if (queue == null) return;

        try {
            IData pipeline = IDataFactory.create();
            IDataCursor cursor = pipeline.getCursor();
            IDataUtil.put(cursor, "queue", queue);
            cursor.destroy();

            Service.doInvoke(UPDATE_QUEUE_SERVICE_NAME, pipeline);
        } catch(Exception ex) {
            ExceptionHelper.raise(ex);
        }
    }

    /**
     * Returns the head of the given delivery queue without dequeuing it.
     *
     * @param queue         The delivery queue whose head job is to be returned.
     * @param ordered       Whether jobs should be dequeued in strict creation datetime first in first out (FIFO) order.
     * @return              The job at the head of the given queue, or null if the queue is empty.
     * @throws SQLException If a database error occurs.
     */
    public static GuaranteedJob peek(DeliveryQueue queue, boolean ordered) throws SQLException {
        if (queue == null) return null;

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet results = null;
        GuaranteedJob job = null;

        try {
            connection = Datastore.getConnection();
            statement = connection.prepareStatement(ordered ? SELECT_NEXT_DELIVERY_JOB_ORDERED_SQL : SELECT_NEXT_DELIVERY_JOB_UNORDERED_SQL);
            statement.clearParameters();

            String queueName = queue.getQueueName();
            SQLWrappers.setChoppedString(statement, 1, queueName, "DeliveryQueue.QueueName");
            SQLWrappers.setChoppedString(statement, 2, queueName, "DeliveryQueue.QueueName");
            SQLWrappers.setTimestamp(statement, 3, new Timestamp(System.currentTimeMillis() - DELIVERY_JOB_AGE_THRESHOLD_MILLISECONDS));

            results = statement.executeQuery();

            if (results.next()) {
                job = GuaranteedJobHelper.get(results.getString(1));
            }

            connection.commit();
        } catch (SQLException ex) {
            connection = Datastore.handleSQLException(connection, ex);
            throw ex;
        } finally {
            SQLWrappers.close(results);
            SQLWrappers.close(statement);
            Datastore.releaseConnection(connection);
        }

        return job;
    }

    /**
     * Dequeues the job at the head of the given delivery queue.
     *
     * @param queue         The delivery queue to dequeue the head job from.
     * @param ordered       Whether jobs should be dequeued in strict creation datetime first in first out (FIFO) order.
     * @return              The dequeued job that was at the head of the given queue, or null if queue is empty.
     * @throws SQLException If a database error occurs.
     */
    public static GuaranteedJob pop(DeliveryQueue queue, boolean ordered) throws SQLException {
        GuaranteedJob job = peek(queue, ordered);
        GuaranteedJobHelper.setDelivering(job);
        return job;
    }

    /**
     * Callable for invoking a given service against a given job.
     */
    private static class CallableGuaranteedJob implements Callable<IData> {
        /**
         * The number of retries when trying to complete a job.
         */
        private static int MAX_RETRIES = 60;
        /**
         * How long to wait between each retry when trying to complete a job.
         */
        private static long WAIT_BETWEEN_RETRIES_MILLISECONDS = 1000;
        /**
         * The job against which the service will be invoked.
         */
        private GuaranteedJob job;

        /**
         * The delivery queue from which the job was dequeued.
         */
        private DeliveryQueue queue;

        /**
         * The service to be invoked.
         */
        private NSName service;

        /**
         * The pipeline the service is invoked with.
         */
        private IData pipeline;

        /**
         * The session the service is invoked under.
         */
        private Session session;

        /**
         * The retry settings to be used when retrying the job.
         */
        private int retryLimit, retryFactor, timeToWait;

        /**
         * Whether the deliver queue should be suspended on retry exhaustion.
         */
        private boolean suspend;

        /**
         * Whether the owning bizdoc's status should be changed to reflect job success/failure.
         */
        private boolean statusSilence;

        /**
         * The time the job was dequeued.
         */
        private long timeDequeued;

        /**
         * The user status a BizDocEnvelope is set to if all deliveries of the job are exhausted.
         */
        private String exhaustedStatus;

        /**
         * Creates a new CallableGuaranteedJob which when called invokes the given service against the given job.
         *
         * @param job           The job to be processed.
         * @param service       The service to be invoked to process the given job.
         * @param session       The session used when invoking the given service.
         * @param pipeline      The input pipeline used when invoking the given service.
         * @param retryLimit    The number of retries this job should attempt.
         * @param retryFactor   The factor used to extend the time to wait on each retry.
         * @param timeToWait    The time in seconds to wait between each retry.
         * @param suspend       Whether to suspend the delivery queue on job retry exhaustion.
         */
        public CallableGuaranteedJob(DeliveryQueue queue, GuaranteedJob job, String service, Session session, IData pipeline, int retryLimit, int retryFactor, int timeToWait, boolean suspend, String exhaustedStatus) {
            this(queue, job, service == null ? null : NSName.create(service), session, pipeline, retryLimit, retryFactor, timeToWait, suspend, exhaustedStatus);
        }

        /**
         * Creates a new CallableGuaranteedJob which when called invokes the given service against the given job.
         *
         * @param job           The job to be processed.
         * @param service       The service to be invoked to process the given job.
         * @param session       The session used when invoking the given service.
         * @param pipeline      The input pipeline used when invoking the given service.
         * @param retryLimit    The number of retries this job should attempt.
         * @param retryFactor   The factor used to extend the time to wait on each retry.
         * @param timeToWait    The time in seconds to wait between each retry.
         * @param suspend       Whether to suspend the delivery queue on job retry exhaustion.
         */
        public CallableGuaranteedJob(DeliveryQueue queue, GuaranteedJob job, NSName service, Session session, IData pipeline, int retryLimit, int retryFactor, int timeToWait, boolean suspend, String exhaustedStatus) {
            if (queue == null) throw new NullPointerException("queue must not be null");
            if (job == null) throw new NullPointerException("job must not be null");
            if (service == null) throw new NullPointerException("service must not be null");

            this.queue = queue;
            this.job = job;
            this.service = service;
            this.session = session;
            this.pipeline = pipeline == null ? IDataFactory.create() : IDataHelper.duplicate(pipeline);
            this.retryLimit = retryLimit;
            this.retryFactor = retryFactor;
            this.timeToWait = timeToWait;
            this.suspend = suspend;
            this.statusSilence = getStatusSilence(queue);
            this.exhaustedStatus = exhaustedStatus;
        }

        /**
         * Invokes the provided service with the provided pipeline and session against the job.
         *
         * @return The output pipeline returned by the invocation.
         * @throws Exception If the service encounters an error.
         */
        public IData call() throws Exception {
            IData output = null;

            Thread owningThread = Thread.currentThread();
            String owningThreadPrefix = owningThread.getName();

            try {
                timeDequeued = System.currentTimeMillis();
                BizDocEnvelope bizdoc = job.getBizDocEnvelope();

                owningThread.setName(MessageFormat.format("{0}: Task={1} Time={2} STARTED", owningThreadPrefix, job.getJobId(), DateTimeHelper.now("datetime")));

                if (bizdoc != null) {
                    BizDocEnvelopeHelper.setStatus(job.getBizDocEnvelope(), null, DEQUEUED_USER_STATUS, statusSilence);
                }

                GuaranteedJobHelper.log(job, "MESSAGE", "Processing", MessageFormat.format("Dequeued from {0} queue \"{1}\"", queue.getQueueType(), queue.getQueueName()), MessageFormat.format("Service \"{0}\" attempting to process document", service.getFullName()));

                IDataCursor cursor = pipeline.getCursor();
                IDataUtil.put(cursor, "$task", job);

                if (bizdoc != null) {
                    bizdoc = BizDocEnvelopeHelper.get(bizdoc.getInternalId(), true);
                    IDataUtil.put(cursor, "bizdoc", bizdoc);
                    IDataUtil.put(cursor, "sender", ProfileCache.getInstance().get(bizdoc.getSenderId()));
                    IDataUtil.put(cursor, "receiver", ProfileCache.getInstance().get(bizdoc.getReceiverId()));
                }

                cursor.destroy();

                output = Service.doInvoke(service, session, pipeline);

                owningThread.setName(MessageFormat.format("{0}: Task={1} Time={2} COMPLETED", owningThreadPrefix, job.getJobId(), DateTimeHelper.now("datetime")));
                setJobCompleted(output);
            } catch(Exception ex) {
                ServerAPI.logError(ex);

                owningThread.setName(MessageFormat.format("{0}: Task={1} Time={2} FAILED: {3}", owningThreadPrefix, job.getJobId(), DateTimeHelper.now("datetime"), ExceptionHelper.getMessage(ex)));
                setJobCompleted(output, ex);

                throw ex;
            } finally {
                owningThread.setName(owningThreadPrefix);
            }

            return output;
        }

        /**
         * Sets the job as successfully completed.
         *
         * @param serviceOutput The output of the service used to process the job.
         * @throws Exception If a database error occurs.
         */
        private void setJobCompleted(IData serviceOutput) throws Exception {
            setJobCompleted(serviceOutput, null);
        }

        /**
         * Sets the job as either successfully or unsuccessfully completed, depending on whether
         * and exception is provided.
         *
         * @param serviceOutput The output of the service used to process the job.
         * @param exception Optional exception encountered while processing the job.
         * @throws Exception If a database error occurs.
         */
        private void setJobCompleted(IData serviceOutput, Throwable exception) throws Exception {
            int retry = 1;

            while(true) {
                try {
                    IData input = IDataFactory.create();

                    IDataCursor cursor = input.getCursor();
                    IDataUtil.put(cursor, "taskid", job.getJobId());
                    IDataUtil.put(cursor, "queue", queue.getQueueName());

                    if (exception == null) {
                        IDataUtil.put(cursor, "status", "success");
                    } else {
                        IDataUtil.put(cursor, "status", "fail");
                        IDataUtil.put(cursor, "statusMsg", ExceptionHelper.getMessage(exception));

                        if (retryLimit > 0 && GuaranteedJobHelper.hasUnrecoverableErrors(job)) {
                            // abort the delivery job so it won't be retried
                            GuaranteedJobHelper.setRetryStrategy(job, 0, 1, 0);
                            GuaranteedJobHelper.log(job, "ERROR", "Delivery", "Delivery aborted", MessageFormat.format("Delivery task \"{0}\" on {1} queue \"{2}\" was aborted due to unrecoverable errors being encountered, and will not be retried", job.getJobId(), queue.getQueueType(), queue.getQueueName()));
                        } else {
                            GuaranteedJobHelper.setRetryStrategy(job, retryLimit, retryFactor, timeToWait);
                        }
                    }

                    IDataUtil.put(cursor, "timeDequeued", timeDequeued);
                    if (serviceOutput != null) IDataUtil.put(cursor, "serviceOutput", serviceOutput);
                    cursor.destroy();

                    Service.doInvoke(UPDATE_QUEUED_TASK_SERVICE_NAME, session, input);

                    GuaranteedJobHelper.retry(job, suspend, exhaustedStatus);

                    break;
                } catch(Exception ex) {
                    ServerAPI.logError(ex);

                    if (retry++ >= MAX_RETRIES) {
                        throw ex;
                    } else {
                        Thread.sleep(WAIT_BETWEEN_RETRIES_MILLISECONDS);
                    }
                }
            }
        }
    }

    /**
     * List of the threads currently processing queues, to prevent multiple processes per queue.
     */
    private static ConcurrentMap<String, Thread> queueProcessingThreads = new ConcurrentHashMap<String, Thread>();

    /**
     * Whether queue processing is started.
     */
    private static volatile boolean isStarted = false;

    /**
     * Starts/enables queue processing.
     */
    public static void start() {
        isStarted = true;
    }

    /**
     * Stops/disables queue processing, and shuts down all currently processing supervisors.
     */
    public static void stop() {
        isStarted = false;
        // stop all threads currently processing queues
        for (Map.Entry<String, Thread> entry : queueProcessingThreads.entrySet()) {
            Thread thread = entry.getValue();
            if (thread != null) {
                thread.interrupt();
            }
        }
    }

    /**
     * Dequeues each task on the given Trading Networks delivery queue, and processes the task using the given service
     * and input pipeline; if concurrency > 1, tasks will be processed by a thread pool whose size is equal to the
     * desired concurrency, otherwise they will be processed on the current thread.
     *
     * @param queueName         The name of the delivery queue whose queued jobs are to be processed.
     * @param service           The service to be invoked to process jobs on the given delivery queue.
     * @param pipeline          The input pipeline used when invoking the given service.
     * @param concurrency       If > 1, this is the number of threads used to process jobs simultaneously.
     * @param retryLimit        The number of retries this job should attempt.
     * @param retryFactor       The factor used to extend the time to wait on each retry.
     * @param timeToWait        The time in seconds to wait between each retry.
     * @param threadPriority    The thread priority used when processing tasks.
     * @param daemonize         If true, all threads will be marked as daemons and execution will not end until the JVM
     *                          shuts down or the TN queue is disabled/suspended.
     * @param ordered           Whether delivery queue jobs should be processed in job creation datetime order.
     * @param suspend           Whether to suspend the delivery queue on job retry exhaustion.
     * @param exhaustedStatus   The user status set on the bizdoc when all retries are exhausted.
     * @throws IOException      If an I/O error is encountered.
     * @throws SQLException     If a database error is encountered.
     * @throws ServiceException If an error is encountered while processing jobs.
     */
    public static void each(String queueName, String service, IData pipeline, int concurrency, int retryLimit, int retryFactor, int timeToWait, int threadPriority, boolean daemonize, boolean ordered, boolean suspend, String exhaustedStatus) throws IOException, SQLException, ServiceException {
        if (queueName == null) throw new NullPointerException("queueName must not be null");
        if (service == null) throw new NullPointerException("service must not be null");

        DeliveryQueue queue = DeliveryQueueHelper.get(queueName);
        if (queue == null) throw new ServiceException("Queue '" + queueName + "' does not exist");

        each(queue, NSName.create(service), pipeline, concurrency, retryLimit, retryFactor, timeToWait, threadPriority, daemonize, ordered, suspend, exhaustedStatus);
    }

    /**
     * Dequeues each task on the given Trading Networks delivery queue, and processes the task using the given service
     * and input pipeline; if concurrency > 1, tasks will be processed by a thread pool whose size is equal to the
     * desired concurrency, otherwise they will be processed on the current thread.
     *
     * @param queue             The delivery queue whose queued jobs are to be processed.
     * @param service           The service to be invoked to process jobs on the given delivery queue.
     * @param pipeline          The input pipeline used when invoking the given service.
     * @param concurrency       If > 1, this is the number of threads used to process jobs simultaneously.
     * @param retryLimit        The number of retries this job should attempt.
     * @param retryFactor       The factor used to extend the time to wait on each retry.
     * @param timeToWait        The time in seconds to wait between each retry.
     * @param threadPriority    The thread priority used when processing tasks.
     * @param daemonize         If true, all threads will be marked as daemons and execution will not end until the JVM
     *                          shuts down or the TN queue is disabled/suspended.
     * @param ordered           Whether delivery queue jobs should be processed in job creation datetime order.
     * @param suspend           Whether to suspend the delivery queue on job retry exhaustion.
     * @param exhaustedStatus   The user status set on the bizdoc when all retries are exhausted.
     * @throws ServiceException If an error is encountered while processing jobs.
     */
    public static void each(DeliveryQueue queue, NSName service, IData pipeline, int concurrency, int retryLimit, int retryFactor, int timeToWait, int threadPriority, boolean daemonize, boolean ordered, boolean suspend, String exhaustedStatus) throws ServiceException {
        if (isStarted) {
            // normalize concurrency
            if (concurrency <= 0) concurrency = 1;

            // only allow one supervisor thread at a time to process a given queue; if a new supervisor is started while
            // there is an existing supervisor, the new supervisor exits immediately
            Thread existingThread = queueProcessingThreads.putIfAbsent(queue.getQueueName(), Thread.currentThread());
            if (existingThread == null) {
                String parentContext = IdentityHelper.generate();

                // set owning thread priority and name
                String previousThreadName = Thread.currentThread().getName();
                int previousThreadPriority = Thread.currentThread().getPriority();
                Thread.currentThread().setPriority(ThreadHelper.normalizePriority(threadPriority));

                String threadName = getThreadPrefix(queue, parentContext);
                if (concurrency > 1) {
                    threadName = threadName + SUPERVISOR_THREAD_SUFFIX;
                } else {
                    threadName = threadName + WORKER_THREAD_SUFFIX;
                }
                Thread.currentThread().setName(threadName);

                boolean invokedByTradingNetworks = invokedByTradingNetworks();
                boolean queueEnabled = queue.isEnabled() || queue.isDraining();

                Session session = Service.getSession();
                ExecutorService executor = getExecutor(queue, concurrency, threadPriority, daemonize, InvokeState.getCurrentState(), parentContext);

                long nextDeliveryQueueRefreshTime = System.currentTimeMillis() + WAIT_BETWEEN_DELIVERY_QUEUE_REFRESH_MILLISECONDS, sleepDuration = 0L;

                try {
                    // while not interrupted and (not invoked by TN or queue is enabled): process queued jobs
                    while (!Thread.interrupted() && (!invokedByTradingNetworks || queueEnabled)) {
                        try {
                            if (sleepDuration > 0L) Thread.sleep(sleepDuration);

                            // set default sleep duration for when there are no pending jobs in queue or all threads are busy
                            sleepDuration = MIN_WAIT_BETWEEN_DELIVERY_QUEUE_POLLS_MILLISECONDS;

                            int activeCount = 0;
                            if (executor instanceof ThreadPoolExecutor) {
                                activeCount = ((ThreadPoolExecutor)executor).getActiveCount();
                            }

                            if (activeCount < concurrency) {
                                GuaranteedJob job = DeliveryQueueHelper.pop(queue, ordered);
                                if (job != null) {
                                    // submit the job to the executor to be processed
                                    executor.submit(new CallableGuaranteedJob(queue, job, service, session, pipeline, retryLimit, retryFactor, timeToWait, suspend, exhaustedStatus));
                                    sleepDuration = 0L; // poll for another job immediately, because the assumption is if there was one pending job then there is probably more
                                } else if (activeCount == 0) {
                                    // no pending jobs, and thread pool is idle
                                    if (daemonize) {
                                        // calculate the next run time based on TN queue schedule so that we can sleep until that time
                                        sleepDuration = untilNextRun(queue);
                                        if (sleepDuration == 0L) {
                                            // either the TN queue schedule was scheduled to run once or if it has now expired, so exit
                                            break;
                                        }
                                    } else {
                                        // if not daemon and all threads have finished and there are no more jobs, then exit
                                        break;
                                    }
                                }
                            }

                            // refresh the delivery queue settings from the database, in case they have changed
                            if (invokedByTradingNetworks && System.currentTimeMillis() >= nextDeliveryQueueRefreshTime) {
                                queue = DeliveryQueueHelper.refresh(queue);
                                queueEnabled = queue.isEnabled() || queue.isDraining();
                                nextDeliveryQueueRefreshTime = System.currentTimeMillis() + WAIT_BETWEEN_DELIVERY_QUEUE_REFRESH_MILLISECONDS;
                            }
                        } catch (InterruptedException ex) {
                            // exit if thread is interrupted
                            break;
                        } catch (ServiceException ex) {
                            // assume exception is recoverable, log it and then continue
                            ServerAPI.logError(ex);
                        } catch (SQLException ex) {
                            // assume exception is recoverable, log it and then continue
                            ServerAPI.logError(ex);
                        } catch (IOException ex) {
                            // assume exception is recoverable, log it and then continue
                            ServerAPI.logError(ex);
                        }
                    }
                } catch (Throwable ex) {
                    ExceptionHelper.raise(ex);
                } finally {
                    try {
                        // shutdown the supervisor and all worker threads
                        executor.shutdown();
                    } finally {
                        // restore owning thread priority and name
                        Thread.currentThread().setPriority(previousThreadPriority);
                        Thread.currentThread().setName(previousThreadName);

                        // remove this thread from the list of queue processing threads
                        queueProcessingThreads.remove(queue.getQueueName(), Thread.currentThread());
                    }
                }
            }
        }
    }

    /**
     * Returns an executor appropriate for the level of desired concurrency.
     *
     * @param queue          The delivery queue to be processed.
     * @param concurrency    The level of desired concurrency.
     * @param threadPriority The thread priority to be used by the returned executor.
     * @param threadDaemon   Whether the created threads should be daemons.
     * @param invokeState    The invoke state to be used by the thread pool.
     * @param parentContext  A unique parent context ID to be included in a thread name for diagnostics.
     * @return               An executor appropriate for the level of desired concurrency.
     */
    private static ExecutorService getExecutor(DeliveryQueue queue, int concurrency, int threadPriority, boolean threadDaemon, InvokeState invokeState, String parentContext) {
        ExecutorService executor;

        if (concurrency <= 1) {
            executor = new DirectExecutorService();
        } else {
            executor = new BlockingServerThreadPoolExecutor(concurrency, getThreadPrefix(queue, parentContext) + WORKER_THREAD_SUFFIX, null, threadPriority, threadDaemon, invokeState);
            ((BlockingServerThreadPoolExecutor)executor).allowCoreThreadTimeOut(true);
        }

        return executor;
    }

    /**
     * Returns the thread name prefix to be used for this delivery queue.
     *
     * @param queue         The queue which will be processed by threads with the returned prefix.
     * @param parentContext A unique parent context ID to be included in a thread name for diagnostics.
     * @return              The thread name prefix used when processing the qiven queue.
     */
    private static String getThreadPrefix(DeliveryQueue queue, String parentContext) {
        String output;

        int truncateLength = 25;

        if (parentContext == null) {
            output = MessageFormat.format("TundraTN/Queue \"{0}\"", StringHelper.truncate(queue.getQueueName(), truncateLength, true));
        } else {
            output = MessageFormat.format("TundraTN/Queue \"{0}\" ParentContext={1}", StringHelper.truncate(queue.getQueueName(), truncateLength, true), parentContext);
        }

        return output;
    }

    /**
     * Returns true if the invocation call stack includes the WmTN/wm.tn.queuing:deliverBatch service.
     *
     * @return True if the invocation call stack includes the WmTN/wm.tn.queuing:deliverBatch service.
     */
    private static boolean invokedByTradingNetworks() {
        java.util.Iterator iterator = InvokeState.getCurrentState().getCallStack().iterator();
        boolean result = false;
        while(iterator.hasNext()) {
            result = iterator.next().toString().equals(DELIVER_BATCH_SERVICE_NAME);
            if (result) break;
        }
        return result;
    }

    /**
     * Returns the number of milliseconds to wait until the next scheduled run of the given delivery queue.
     *
     * @param  queue            A delivery queue.
     * @return                  The number of milliseconds to wait.
     * @throws ServiceException If a datetime parsing error occurs.
     */
    private static long untilNextRun(DeliveryQueue queue) throws ServiceException {
        long next = nextRun(queue);
        long now = System.currentTimeMillis();
        return next > now ? next - now : 0L;
    }

    /**
     * Parser for the datetimes to be parsed in a DeliverySchedule object.
     */
    private static final SimpleDateFormat DELIVERY_SCHEDULE_DATETIME_PARSER = new SimpleDateFormat("yyyy/MM/ddHH:mm:ss");

    /**
     * Returns the time in milliseconds of the next scheduled run of the given delivery queue.
     *
     * @param  queue            A delivery queue.
     * @return                  The time in milliseconds of the next scheduled run.
     * @throws ServiceException If a datetime parsing error occurs.
     */
    private static long nextRun(DeliveryQueue queue) throws ServiceException {
        DeliverySchedule schedule = queue.getSchedule();
        String type = schedule.getType();

        long next = 0L, start = 0L, end = 0L;

        try {
            String endDate = schedule.getEndDate(), endTime = schedule.getEndTime();
            if (endDate != null && endTime != null) {
                end = DELIVERY_SCHEDULE_DATETIME_PARSER.parse(endDate + endTime).getTime();
            }

            boolean noOverlap = BooleanHelper.parse(schedule.getNoOverlap());

            if (type.equals(DeliverySchedule.TYPE_REPEATING)) {
                ScheduledTask.Simple repeatingTask = new ScheduledTask.Simple(Long.parseLong(schedule.getInterval()) * 1000L, noOverlap, start, end);

                if (!repeatingTask.isExpired()) {
                    repeatingTask.calcNextTime();
                    next = repeatingTask.getNextRun();
                }
            } else if (type.equals(DeliverySchedule.TYPE_COMPLEX)) {
                ScheduledTask.Mask complexTask = new ScheduledTask.Mask(Masks.buildLongMask(schedule.getMinutes()),
                                                                        Masks.buildIntMask(schedule.getHours()),
                                                                        Masks.buildIntMask(schedule.getDaysOfMonth()),
                                                                        Masks.buildIntMask(schedule.getDaysOfWeek()),
                                                                        Masks.buildIntMask(schedule.getMonths()),
                                                                        noOverlap, start, end);

                if (!complexTask.isExpired()) {
                    complexTask.calcNextTime();
                    next = complexTask.getNextRun();
                }
            }
        } catch(ParseException ex) {
            ExceptionHelper.raise(ex);
        }

        return next;
    }

    /**
     * Returns whether bizdoc status should be changed or not.
     *
     * @param queue The queue check for status silence on.
     * @return      True if bizdoc status should not be changed, otherwise false.
     */
    public static boolean getStatusSilence(DeliveryQueue queue) {
        boolean statusSilence = false;

        if (queue != null) {
            DeliverySchedule schedule = queue.getSchedule();

            if (schedule != null) {
                IData pipeline = schedule.getInputs();
                if (pipeline != null) {
                    IDataCursor cursor = pipeline.getCursor();
                    try {
                        statusSilence = BooleanHelper.parse(IDataUtil.getString(cursor, "$status.silence?"));
                    } finally {
                        cursor.destroy();
                    }
                }
            }
        }
        return statusSilence;
    }

    /**
     * Converts the given Trading Networks delivery queue to an IData doc.
     *
     * @param input The queue to convert to an IData doc representation.
     * @return      An IData doc representation of the given queue.
     * @throws ServiceException If a database error occurs.
     */
    public static IData toIData(DeliveryQueue input) throws ServiceException {
        if (input == null) return null;

        IData output = IDataFactory.create();
        IDataCursor cursor = output.getCursor();

        IDataUtil.put(cursor, "name", input.getQueueName());
        IDataUtil.put(cursor, "type", input.getQueueType());
        IDataUtil.put(cursor, "status", input.getState());
        IDataUtil.put(cursor, "length", "" + length(input));

        cursor.destroy();

        return output;
    }

    /**
     * Converts the given list of Trading Networks delivery queues to an IData[] doc list.
     *
     * @param input The list of queues to convert to an IData[] doc list representation.
     * @return      An IData[] doc list representation of the given queues.
     * @throws ServiceException If a database error occurs.
     */
    public static IData[] toIDataArray(DeliveryQueue[] input) throws ServiceException {
        if (input == null) return null;

        IData[] output = new IData[input.length];

        for (int i = 0; i < input.length; i++) {
            output[i] = toIData(input[i]);
        }

        return output;
    }
}
