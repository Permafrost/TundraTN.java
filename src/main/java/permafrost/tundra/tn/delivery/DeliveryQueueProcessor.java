package permafrost.tundra.tn.delivery;

import com.wm.app.b2b.server.InvokeState;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.b2b.server.Session;
import com.wm.app.tn.delivery.DeliveryQueue;
import com.wm.app.tn.delivery.GuaranteedJob;
import com.wm.data.IData;
import com.wm.lang.ns.NSName;
import permafrost.tundra.data.IDataMap;
import permafrost.tundra.id.ULID;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.lang.StringHelper;
import permafrost.tundra.lang.ThreadHelper;
import permafrost.tundra.server.BlockingServerThreadPoolExecutor;
import permafrost.tundra.time.DateTimeHelper;
import permafrost.tundra.util.concurrent.DirectExecutorService;
import java.io.IOException;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import javax.xml.datatype.Duration;

/**
 * Processes jobs on a TN delivery queue via a bizdoc processing service.
 */
public class DeliveryQueueProcessor {
    /**
     * The name of the service that Trading Networks uses to invoke delivery queue processing services.
     */
    private static final String DELIVER_BATCH_SERVICE_NAME = "wm.tn.queuing:deliverBatch";
    /**
     * The minimum wait between each poll of a delivery queue for more jobs.
     */
    private static final long MIN_WAIT_BETWEEN_DELIVERY_QUEUE_POLLS_MILLISECONDS = 1L;
    /**
     * The wait after a poll of an empty delivery queue until we poll again for more jobs.
     */
    private static final long WAIT_AFTER_EMPTY_DELIVERY_QUEUE_POLL_MILLISECONDS = 500L;
    /**
     * The wait between each refresh of a delivery queue settings from the database.
     */
    private static final long WAIT_BETWEEN_DELIVERY_QUEUE_REFRESH_MILLISECONDS = 5000L;
    /**
     * The suffix used on worker thread names.
     */
    private static final String WORKER_THREAD_SUFFIX = ": Worker";
    /**
     * The suffix used on supervisor thread names.
     */
    private static final String SUPERVISOR_THREAD_SUFFIX = ": Supervisor";
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
     * Interrupts and therefore shuts down the processing supervisor associated with the given queue.
     *
     * @param queueName The name of the queue whose processing is to be interrupted.
     */
    public static void interrupt(String queueName) {
        if (queueName == null) return;
        Thread thread = queueProcessingThreads.remove(queueName);
        if (thread != null) {
            thread.interrupt();
        }
    }

    /**
     * Returns whether queue processing is started/enabled on this Integration Server.
     *
     * @return Whether queue processing is started/enabled on this Integration Server.
     */
    public static boolean isStarted() {
        return isStarted;
    }

    /**
     * Returns a list of all currently processing queues and the associated processing thread.
     *
     * @return                  A list of all currently processing queues and the associated processing thread.
     * @throws IOException      If an I/O error occurs.
     * @throws ServiceException If a service invocation error occurs.
     * @throws SQLException     If a database error occurs.
     */
    public static IData[] list() throws IOException, ServiceException, SQLException {
        List<IData> output = new ArrayList<IData>(queueProcessingThreads.size());

        for (Map.Entry<String, Thread> entry : queueProcessingThreads.entrySet()) {
            IDataMap map = new IDataMap();
            map.put("queue", DeliveryQueueHelper.toIData(DeliveryQueueHelper.get(entry.getKey())));
            map.put("thread", ThreadHelper.toIData(entry.getValue()));
            output.add(map);
        }

        return output.toArray(new IData[output.size()]);
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
     * @throws IOException      If an I/O error is encountered.
     * @throws SQLException     If a database error is encountered.
     * @throws ServiceException If an error is encountered while processing jobs.
     */
    public static void each(String queueName, String service, IData pipeline, Duration age, int concurrency, int retryLimit, float retryFactor, Duration timeToWait, int threadPriority, boolean daemonize, boolean ordered, boolean suspend, String exhaustedStatus) throws IOException, SQLException, ServiceException {
        if (queueName == null) throw new NullPointerException("queueName must not be null");
        if (service == null) throw new NullPointerException("service must not be null");

        DeliveryQueue queue = DeliveryQueueHelper.get(queueName);
        if (queue == null) throw new ServiceException("Queue '" + queueName + "' does not exist");

        each(queue, NSName.create(service), pipeline, age, concurrency, retryLimit, retryFactor, timeToWait, threadPriority, daemonize, ordered, suspend, exhaustedStatus);
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
     * @throws ServiceException If an error is encountered while processing jobs.
     */
    public static void each(DeliveryQueue queue, NSName service, IData pipeline, Duration age, int concurrency, int retryLimit, float retryFactor, Duration timeToWait, int threadPriority, boolean daemonize, boolean ordered, boolean suspend, String exhaustedStatus) throws ServiceException {
        if (isStarted) {
            // normalize concurrency
            if (concurrency <= 0) concurrency = 1;
            // normalize retryFactor
            if (retryFactor < 1.0f) retryFactor = 1.0f;

            // only allow one supervisor thread at a time to process a given queue; if a new supervisor is started while
            // there is an existing supervisor, the new supervisor exits immediately
            Thread existingThread = queueProcessingThreads.putIfAbsent(queue.getQueueName(), Thread.currentThread());
            if (existingThread == null) {
                String parentContext = ULID.generate();

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
                    boolean didPreviousPollProcessJob = false;

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
                                GuaranteedJob job = DeliveryQueueHelper.pop(queue, ordered, age);
                                if (job != null) {
                                    // submit the job to the executor to be processed
                                    executor.submit(new CallableGuaranteedJob(queue, job, service, session, pipeline, retryLimit, retryFactor, timeToWait, suspend, exhaustedStatus));
                                    didPreviousPollProcessJob = true;
                                    sleepDuration = 0L; // poll for another job immediately, because the assumption is if there was one pending job then there is probably more
                                } else if (activeCount == 0) {
                                    // no pending jobs, and thread pool is idle
                                    if (didPreviousPollProcessJob) {
                                        sleepDuration = WAIT_AFTER_EMPTY_DELIVERY_QUEUE_POLL_MILLISECONDS;
                                    } else if (daemonize) {
                                        // calculate the next run time based on TN queue schedule so that we can sleep until that time
                                        sleepDuration = untilNextRun(queue);
                                        if (sleepDuration == 0L) {
                                            // either the TN queue schedule was scheduled to run once or it has now expired, so exit
                                            break;
                                        }
                                    } else {
                                        // if not daemon and all threads have finished and there are no more jobs, then exit
                                        break;
                                    }
                                    didPreviousPollProcessJob = false;
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

        String queueName = StringHelper.truncate(queue.getQueueName(), 25, true);
        String datetime = DateTimeHelper.now("datetime");

        if (parentContext == null) {
            output = MessageFormat.format("TundraTN/Queue \"{0}\" ParentStart={1}", queueName, datetime);
        } else {
            output = MessageFormat.format("TundraTN/Queue \"{0}\" ParentStart={1} ParentContext={2}", queueName, datetime, parentContext);
        }

        return output;
    }

    /**
     * Returns true if the invocation call stack includes the WmTN/wm.tn.queuing:deliverBatch service.
     *
     * @return True if the invocation call stack includes the WmTN/wm.tn.queuing:deliverBatch service.
     */
    private static boolean invokedByTradingNetworks() {
        Iterator iterator = InvokeState.getCurrentState().getCallStack().iterator();
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
        long next = DeliveryQueueHelper.nextRun(queue);
        long now = System.currentTimeMillis();
        return next > now ? next - now : 0L;
    }
}
