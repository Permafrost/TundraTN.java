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
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.b2b.server.Session;
import com.wm.app.tn.delivery.DeliveryQueue;
import com.wm.app.tn.delivery.GuaranteedJob;
import com.wm.data.IData;
import com.wm.lang.ns.NSName;
import com.wm.util.coder.IDataCodable;
import permafrost.tundra.data.IDataMap;
import permafrost.tundra.id.UUIDHelper;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.lang.Startable;
import permafrost.tundra.lang.StringHelper;
import permafrost.tundra.lang.ThreadHelper;
import permafrost.tundra.server.ServerThread;
import permafrost.tundra.server.ServerThreadPoolExecutor;
import permafrost.tundra.time.DateTimeHelper;
import permafrost.tundra.util.concurrent.DirectExecutorService;
import javax.xml.datatype.Duration;
import java.text.MessageFormat;
import java.util.ArrayDeque;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Trading Networks Queue processor that supports multiple threads, task prioritization, task retrying, continuous
 * failure detection to backoff task processing when required.
 */
public class DeliveryQueueProcessor implements Startable, IDataCodable {
    /**
     * The name of the service that Trading Networks uses to invoke delivery queue processing services.
     */
    private static final String DELIVER_BATCH_SERVICE_NAME = "wm.tn.queuing:deliverBatch";
    /**
     * The default wait between each poll of a delivery queue for more jobs.
     */
    private static final long WAIT_BETWEEN_DELIVERY_QUEUE_POLLS_MILLISECONDS = 100L;
    /**
     * The default wait after a poll of an empty delivery queue until we poll again for more jobs.
     */
    private static final long WAIT_AFTER_EMPTY_DELIVERY_QUEUE_POLL_MILLISECONDS = 1000L;
    /**
     * The timeout used when waiting for tasks to complete while shutting down the executor.
     */
    private static final long EXECUTOR_SHUTDOWN_TIMEOUT_MILLISECONDS = 5 * 60 * 1000L;
    /**
     * The factor used to determine task queue refill size when refilling the queue.
     */
    public static final int DEFAULT_QUEUE_REFILL_SIZE_FACTOR = 32;
    /**
     * The factor used to determine the task queue level at which to refill the queue.
     */
    public static final int DEFAULT_QUEUE_REFILL_LEVEL_FACTOR = 2;
    /**
     * The executor service used to process tasks.
     */
    private ExecutorService executorService;
    /**
     * The continuous failure detector used to back off when tasks are continually failing beyond a given threshold.
     */
    private final ContinuousFailureDetector continuousFailureDetector;
    /**
     * If greater than 1, this is the number of threads used to process jobs simultaneously.
     */
    private final int concurrency;
    /**
     * The level at which the task queue will be refilled.
     */
    private final int refillLevel;
    /**
     * The size to which the task queue will be refilled.
     */
    private final int refillSize;
    /**
     * The factor used to extend the time to wait on each retry.
     */
    private final float retryFactor;
    /**
     * The number of retries this job should attempt.
     */
    private final int retryLimit;
    /**
     * Whether this processor was invoked directly by Trading Networks vs invoked manually by a user.
     */
    private final boolean invokedByTradingNetworks;
    /**
     * The session used when invoking the given service to process a task.
     */
    private final Session session;
    /**
     * The parent or supervisor context used in thread names for correlation and diagnostics.
     */
    private final String parentContext;
    /**
     * The name of the delivery queue whose queued jobs are to be processed.
     */
    private final DeliveryQueue queue;
    /**
     * The thread priority used when processing tasks.
     */
    private final int threadPriority;
    /**
     * If true, all threads will be marked as daemons and execution will not end until the JVM shuts down or the TN
     * queue is disabled/suspended.
     */
    private final boolean daemonize;
    /**
     * Whether to process tasks in strict task creation order.
     */
    private final boolean ordered;
    /**
     * Whether to suspend the queue on task exhaustion.
     */
    private final boolean suspend;
    /**
     * The status used when exhausting all retries of a task.
     */
    private final String exhaustedStatus;
    /**
     * Optional service to be invoked when all retries are exhausted.
     */
    private final NSName exhaustedService;
    /**
     * The service to be invoked to process jobs on the given delivery queue.
     */
    private final NSName service;
    /**
     * The invoke status used when invoking the given service to process a task.
     */
    private final IData pipeline;
    /**
     * The minimum age a task must be before it is processed.
     */
    private final Duration age;
    /**
     * The time to wait between each retry.
     */
    private final Duration timeToWait;
    /**
     * The invoke status used when invoking the given service to process a task.
     */
    private final InvokeState invokeState;
    /**
     * Whether the processor is started.
     */
    private volatile boolean started;
    /**
     * The thread being used to process tasks.
     */
    private volatile Thread processingThread;

    /**
     * Construct a new DeliveryQueueProcessor.
     *
     * @param queue             The name of the delivery queue whose queued jobs are to be processed.
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
     * @param ordered           Whether to process tasks in strict task creation order.
     * @param suspend           Whether to suspend the queue on task exhaustion.
     * @param exhaustedStatus   The status used when exhausting all retries of a task.
     * @param exhaustedService  Optional service to be invoked when all retries are exhausted.
     * @param errorThreshold    The threshold of continuous task failures at which backing off of task processing occurs.
     */
    public DeliveryQueueProcessor(DeliveryQueue queue, NSName service, IData pipeline, Duration age, int concurrency, int retryLimit, float retryFactor, Duration timeToWait, int threadPriority, boolean daemonize, boolean ordered, boolean suspend, String exhaustedStatus, NSName exhaustedService, long errorThreshold) {
        this.concurrency = Math.max(concurrency, 1);
        this.retryLimit = retryLimit;
        this.retryFactor = Math.max(retryFactor, 1.0f);
        this.queue = queue;
        this.threadPriority = threadPriority;
        this.daemonize = daemonize;
        this.suspend = suspend;
        this.exhaustedStatus = exhaustedStatus;
        this.exhaustedService = exhaustedService;
        this.service = service;
        this.pipeline = pipeline;
        this.age = age;
        this.timeToWait = timeToWait;
        this.ordered = ordered;
        this.invokeState = InvokeState.getCurrentState();
        this.parentContext = UUIDHelper.generate();
        this.invokedByTradingNetworks = wasInvokedByTradingNetworks();
        this.session = Service.getSession();
        this.refillLevel = this.ordered ? 1 : this.concurrency * DEFAULT_QUEUE_REFILL_LEVEL_FACTOR;
        this.refillSize = this.concurrency * DEFAULT_QUEUE_REFILL_SIZE_FACTOR;
        this.continuousFailureDetector = new ContinuousFailureDetector(errorThreshold, this.timeToWait == null ? 0 : this.timeToWait.getTimeInMillis(Calendar.getInstance()));
    }

    /**
     * Processes the queue if processing is started. Calling this method blocks until all tasks have been processed.
     */
    public void process() {
        if (started) {
            processingThread = Thread.currentThread();
            String previousThreadName = processingThread.getName();
            int previousThreadPriority = processingThread.getPriority();

            try {
                // set owning thread priority and name
                processingThread.setPriority(ThreadHelper.normalizePriority(threadPriority));
                processingThread.setName(getSupervisorName());

                continuousFailureDetector.start();

                long sleepDuration = 0L;

                Queue<CallableGuaranteedJob> tasks;
                if (concurrency > 1) {
                    tasks = new ArrayDeque<CallableGuaranteedJob>();
                } else {
                    // tasks need to be prioritized ahead of time when using current thread to execute them
                    tasks = new PriorityQueue<CallableGuaranteedJob>();
                }

                Map<String, Future<IData>> submittedTasks = new HashMap<String, Future<IData>>();
                boolean queueHadTasks = false, hadContinuousFailure = false;

                // while started and not interrupted and not failed continuously and (not invoked by TN or queue is enabled): process queued jobs
                while (started && !Thread.interrupted()) {
                    try {
                        if (sleepDuration > 0L) Thread.sleep(sleepDuration);

                        // set default sleep duration for when there are no pending jobs in queue or all threads are busy
                        sleepDuration = WAIT_BETWEEN_DELIVERY_QUEUE_POLLS_MILLISECONDS;

                        int queueSize = 0, activeCount = 0;
                        if (executorService instanceof ThreadPoolExecutor) {
                            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor)executorService;
                            queueSize = threadPoolExecutor.getQueue().size();
                            activeCount = threadPoolExecutor.getActiveCount();
                            if (continuousFailureDetector.isBackingOff()) {
                                // if we are in a continuous failure state, reduce the pool size to one thread to
                                // reduce compute resource usage during this continuous failure phase
                                threadPoolExecutor.setCorePoolSize(1);
                                threadPoolExecutor.setMaximumPoolSize(1);
                                hadContinuousFailure = true;
                            } else if (hadContinuousFailure) {
                                // if we were previously in a continuous failure state but it has now passed, ramp
                                // the thread pool back up to its normal size
                                threadPoolExecutor.setCorePoolSize(concurrency);
                                threadPoolExecutor.setMaximumPoolSize(concurrency);
                                hadContinuousFailure = false;
                            }
                        }

                        if (activeCount < concurrency || queueSize < refillLevel) {
                            if (tasks.size() == 0) {
                                List<GuaranteedJob> pendingTasks = null;

                                // remove any finished tasks from the list of submitted tasks
                                Iterator<Map.Entry<String, Future<IData>>> iterator = submittedTasks.entrySet().iterator();
                                while (iterator.hasNext()) {
                                    if (iterator.next().getValue().isDone()) {
                                        iterator.remove();
                                    }
                                }

                                if (ordered) {
                                    // only dequeue another task when there are idle threads
                                    if (activeCount < concurrency) {
                                        GuaranteedJob job = DeliveryQueueHelper.pop(queue, true, age);
                                        if (job != null) {
                                            pendingTasks = Collections.singletonList(job);
                                        }
                                    }
                                } else {
                                    pendingTasks = DeliveryQueueHelper.peek(queue, false, age, submittedTasks.keySet(), refillSize - queueSize);
                                }

                                if (pendingTasks != null && pendingTasks.size() > 0) {
                                    for (GuaranteedJob pendingTask : pendingTasks) {
                                        tasks.add(new CallableGuaranteedJob(queue, pendingTask, service, session, pipeline, retryLimit, retryFactor, timeToWait, suspend, exhaustedStatus, exhaustedService, continuousFailureDetector));
                                    }
                                    // refresh thread startTime whenever new tasks are added
                                    if (processingThread instanceof ServerThread) {
                                        ((ServerThread)processingThread).setStartTime();
                                    }
                                }
                            }

                            if (tasks.size() > 0) {
                                CallableGuaranteedJob task;
                                while ((task = tasks.poll()) != null) {
                                    Future<IData> future = executorService.submit(task);
                                    submittedTasks.put(task.getJobIdentity(), future);
                                    // when single-threaded, submit only the head task to the executor to be
                                    // processed, so that this thread can then check if any exit criteria is
                                    // met between tasks
                                    if (concurrency == 1) break;
                                }
                                queueHadTasks = true;

                                // don't wait between task submissions when there are still tasks to be processed
                                if (ordered || tasks.size() > 0) sleepDuration = 0;
                            } else if (activeCount == 0 && queueSize == 0) {
                                if (queueHadTasks) {
                                    // poll again after a short timed wait, because if the queue had tasks
                                    // previously then it is likely it will have more shortly
                                    sleepDuration = WAIT_AFTER_EMPTY_DELIVERY_QUEUE_POLL_MILLISECONDS;
                                } else if (daemonize) {
                                    // calculate the next run time based on TN queue schedule so that we can sleep until that time
                                    sleepDuration = durationToNextRun();
                                    if (sleepDuration == 0L) {
                                        // either the TN queue schedule was scheduled to run once or it has now expired, so exit
                                        break;
                                    }
                                } else {
                                    // if not daemon and all threads have finished and there are no more jobs, then exit
                                    break;
                                }
                                queueHadTasks = false;
                            }
                        }
                    } catch (InterruptedException ex) {
                        // exit if thread is interrupted
                        break;
                    }
                }
            } catch (Throwable ex) {
                ExceptionHelper.raiseUnchecked(ex);
            } finally {
                // restore owning thread priority and name
                processingThread.setPriority(previousThreadPriority);
                processingThread.setName(previousThreadName);
            }
        }
    }

    /**
     * Returns the delivery queue this processor is processing.
     *
     * @return The delivery queue this processor is processing.
     */
    public DeliveryQueue getDeliveryQueue() {
        return queue;
    }

    /**
     * Returns true if this processor was invoked by Trading Networks versus invoked manually.
     *
     * @return true if this processor was invoked by Trading Networks versus invoked manually.
     */
    public boolean isInvokedByTradingNetworks() {
        return invokedByTradingNetworks;
    }

    /**
     * Returns true if this processor is started.
     *
     * @return true if this processor is started.
     */
    @Override
    public boolean isStarted() {
        return started;
    }

    /**
     * Starts queue processing.
     */
    @Override
    public synchronized void start() {
        if (!started) {
            started = true;
            continuousFailureDetector.start();
            executorService = getExecutor();
        }
    }

    /**
     * Stops queue processing.
     */
    @Override
    public synchronized void stop() {
        if (started) {
            started = false;
            if (processingThread != null) {
                processingThread.interrupt();
            }
            shutdown();
            executorService = null;
        }
    }

    /**
     * Restarts queue processing.
     */
    public synchronized void restart() {
        stop();
        start();
    }

    /**
     * Performs an orderly shutdown of this queue processor.
     */
    private void shutdown() {
        if (executorService != null) {
            try {
                if (!ordered && executorService instanceof ThreadPoolExecutor) {
                    // discard any tasks not yet executing
                    ((ThreadPoolExecutor)executorService).getQueue().clear();
                }
                executorService.shutdown();
                continuousFailureDetector.stop();
                executorService.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS);
                executorService.shutdownNow();
            } catch (InterruptedException ex) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Returns an executor appropriate for the level of desired concurrency.
     *
     * @return               An executor appropriate for the level of desired concurrency.
     */
    private ExecutorService getExecutor() {
        ExecutorService executor;
        if (concurrency <= 1) {
            executor = new DirectExecutorService();
        } else {
            executor = new ServerThreadPoolExecutor(concurrency, getWorkerName(), null, threadPriority, daemonize, invokeState, new PriorityBlockingQueue<Runnable>(), new ThreadPoolExecutor.AbortPolicy());
            ((ThreadPoolExecutor)executor).allowCoreThreadTimeOut(true);
        }
        return executor;
    }

    /**
     * Returns the thread name prefix to be used for this delivery queue.
     *
     * @return              The thread name prefix used when processing the qiven queue.
     */
    private String getThreadPrefix() {
        String output;

        String queueName = StringHelper.truncate(queue.getQueueName(), 25, true);
        String datetime = DateTimeHelper.now("datetime");

        if (parentContext == null) {
            output = MessageFormat.format("TundraTN/Queue Worker {0} {1}", queueName, datetime);
        } else {
            output = MessageFormat.format("TundraTN/Queue Worker {0} {1} {2}", queueName, parentContext, datetime);
        }

        return output;
    }

    /**
     * Returns the thread name to use for the supervising thread.
     *
     * @return the thread name to use for the supervising thread.
     */
    private String getSupervisorName() {
        return getThreadPrefix() + (concurrency > 1 ? " Producer" : "");
    }

    /**
     * Returns the thread name to use for the worker threads.
     *
     * @return the thread name to use for the worker threads
     */
    private String getWorkerName() {
        return getThreadPrefix() + " Consumer";
    }

    /**
     * Returns true if the invocation call stack includes the WmTN/wm.tn.queuing:deliverBatch service.
     *
     * @return True if the invocation call stack includes the WmTN/wm.tn.queuing:deliverBatch service.
     */
    private boolean wasInvokedByTradingNetworks() {
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
     * @return                  The number of milliseconds to wait.
     * @throws ServiceException If a datetime parsing error occurs.
     */
    private long durationToNextRun() throws ServiceException {
        long next = DeliveryQueueHelper.nextRun(queue);
        long now = System.currentTimeMillis();
        return next > now ? next - now : 0L;
    }

    /**
     * Returns an IData representation of this object.
     *
     * @return an IData representation of this object.
     */
    @Override
    public IData getIData() {
        IDataMap document = new IDataMap();
        try {
            document.put("started?", isStarted());
            document.put("queue", DeliveryQueueHelper.toIData(queue));
            Thread thread = processingThread;
            if (thread != null) document.put("thread", ThreadHelper.toIData(thread));

            long totalSuccessCount = continuousFailureDetector.getTotalSuccessCount();
            long totalFailureCount = continuousFailureDetector.getTotalFailureCount();
            long currentBackoffCount = continuousFailureDetector.getCurrentBackoffCount();
            long currentContinuousFailureCount = continuousFailureDetector.getCurrentContinuousFailureCount();

            document.put("task.total.count", totalSuccessCount + totalFailureCount);
            document.put("task.success.count", totalSuccessCount);
            document.put("task.failure.count", totalFailureCount);
            document.put("task.failure.continuous.count", currentContinuousFailureCount);
            document.put("task.backoff.count", currentBackoffCount);
        } catch(ServiceException ex) {
            ExceptionHelper.raiseUnchecked(ex);
        }
        return document;
    }

    /**
     * Method not implemented.
     *
     * @param document                          The IData document.
     * @throws UnsupportedOperationException    Method not implemented.
     */
    @Override
    public void setIData(IData document) {
        throw new UnsupportedOperationException("setIData(IData) not implemented");
    }
}
