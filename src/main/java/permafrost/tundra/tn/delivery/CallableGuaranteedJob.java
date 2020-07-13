/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 Lachlan Dowding
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
import com.wm.app.b2b.server.ServiceThread;
import com.wm.app.b2b.server.Session;
import com.wm.app.b2b.server.User;
import com.wm.app.b2b.server.ns.Namespace;
import com.wm.app.tn.delivery.DeliveryJob;
import com.wm.app.tn.delivery.DeliveryQueue;
import com.wm.app.tn.delivery.GuaranteedJob;
import com.wm.app.tn.delivery.QueuingUtils;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.lang.ns.NSName;
import com.wm.lang.ns.NSService;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.lang.IterableHelper;
import permafrost.tundra.lang.StringHelper;
import permafrost.tundra.server.ServerLogger;
import permafrost.tundra.server.ServiceHelper;
import permafrost.tundra.time.DateTimeHelper;
import permafrost.tundra.time.DurationHelper;
import permafrost.tundra.time.DurationPattern;
import permafrost.tundra.tn.document.BizDocEnvelopeHelper;
import permafrost.tundra.tn.document.BizDocEnvelopePriority;
import permafrost.tundra.tn.log.EntryType;
import permafrost.tundra.tn.profile.ProfileCache;
import permafrost.tundra.util.concurrent.AbstractPrioritizedCallable;
import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.xml.datatype.Duration;

/**
 * Callable for invoking a given service against a given job.
 */
public class CallableGuaranteedJob extends AbstractPrioritizedCallable<IData> {
    /**
     * The bizdoc user status to use when a job is dequeued.
     */
    private static final String DEQUEUED_USER_STATUS = "DEQUEUED";
    /**
     * The transport status message character length supported by Trading Networks.
     */
    private static final int TRANSPORT_STATUS_MESSAGE_LENGTH = 512;
    /**
     * The number of retries when trying to complete a job.
     */
    private static final int MAX_RETRIES = 60;
    /**
     * How long to wait between each retry when trying to complete a job.
     */
    private static final long WAIT_BETWEEN_RETRIES_MILLISECONDS = 1000L;
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
     * The maximum number of retries.
     */
    private int retryLimit;
    /**
     * The time to wait between retries.
     */
    private Duration timeToWait;
    /**
     * The retry factor to be used when retrying the job.
     */
    private float retryFactor;
    /**
     * Whether the deliver queue should be suspended on retry exhaustion.
     */
    private boolean suspend;
    /**
     * Whether the owning bizdoc's status should be changed to reflect job success/failure.
     */
    private boolean statusSilence;
    /**
     * The user status a BizDocEnvelope is set to if all deliveries of the job are exhausted.
     */
    private String exhaustedStatus;
    /**
     * Used for detecting if a delivery queue has continuous failure and should terminate.
     */
    private ContinuousFailureDetector continuousFailureDetector;
    /**
     * Whether the job is already dequeued or not.
     */
    private boolean alreadyDequeued;

    /**
     * Creates a new CallableGuaranteedJob which when called invokes the given service against the given job.
     *
     * @param queue             The delivery queue on which the job queued.
     * @param job               The job to be processed.
     * @param service           The service to be invoked to process the given job.
     * @param session           The session used when invoking the given service.
     * @param pipeline          The input pipeline used when invoking the given service.
     * @param retryLimit        The number of retries this job should attempt.
     * @param retryFactor       The factor used to extend the time to wait on each retry.
     * @param timeToWait        The time to wait between each retry.
     * @param suspend           Whether to suspend the delivery queue on job retry exhaustion.
     * @param exhaustedStatus   The status set on the related bizdoc when all retries of the job are exhausted.
     */
    public CallableGuaranteedJob(DeliveryQueue queue, GuaranteedJob job, NSName service, Session session, IData pipeline, int retryLimit, float retryFactor, Duration timeToWait, boolean suspend, String exhaustedStatus, ContinuousFailureDetector continuousFailureDetector) {
        if (queue == null) throw new NullPointerException("queue must not be null");
        if (job == null) throw new NullPointerException("job must not be null");
        if (service == null) throw new NullPointerException("service must not be null");
        if (retryFactor < 1.0f) throw new IllegalArgumentException("retryFactor must not be less than one");
        if (continuousFailureDetector == null) throw new NullPointerException("continuousFailureDetector must not be null");

        this.queue = queue;
        this.job = job;
        this.service = service;
        this.session = session;
        this.pipeline = pipeline == null ? IDataFactory.create() : IDataHelper.duplicate(pipeline);
        this.retryLimit = retryLimit;
        this.retryFactor = retryFactor;
        this.timeToWait = timeToWait;
        this.suspend = suspend;
        this.statusSilence = DeliveryQueueHelper.getStatusSilence(queue);
        this.exhaustedStatus = exhaustedStatus;
        this.continuousFailureDetector = continuousFailureDetector;
        this.alreadyDequeued = job.isDelivering();

        BizDocEnvelope bizdoc = job.getBizDocEnvelope();
        if (bizdoc != null) {
            this.priority = new BizDocEnvelopePriority(bizdoc, 1, TimeUnit.DAYS);
        }
    }

    /**
     * Returns this GuaranteedJob's identity.
     *
     * @return this GuaranteedJob's identity.
     */
    public String getJobIdentity() {
        return job.getJobId();
    }

    /**
     * Invokes the provided service with the provided pipeline and session against the job.
     *
     * @return              The output pipeline returned by the invocation.
     * @throws Exception    If the service encounters an error.
     */
    public IData call() throws Exception {
        IData output = null;

        // backoff if in a continues failure state
        continuousFailureDetector.backoffIfRequired();

        // only execute the task if we're still started after backoff
        if (continuousFailureDetector.isStarted()) {
            long startTime = System.nanoTime();

            Exception exception = null;

            Thread owningThread = Thread.currentThread();
            String owningThreadPrefix = owningThread.getName();
            String jobLogString = GuaranteedJobHelper.toLogString(job);
            boolean requiresCompletion = false;

            try {
                if (alreadyDequeued || GuaranteedJobHelper.setDelivering(job)) {
                    requiresCompletion = true;

                    BizDocEnvelope bizdoc = job.getBizDocEnvelope();

                    owningThread.setName(MessageFormat.format("{0}: Task {1} PROCESSING {2}", owningThreadPrefix, jobLogString, DateTimeHelper.now("datetime")));

                    if (bizdoc != null) {
                        BizDocEnvelopeHelper.setStatus(job.getBizDocEnvelope(), null, DEQUEUED_USER_STATUS, statusSilence);
                    }

                    GuaranteedJobHelper.log(job, EntryType.MESSAGE, "Processing", MessageFormat.format("Dequeued from {0} queue {1}", queue.getQueueType(), queue.getQueueName()), MessageFormat.format("Service {0} attempting to process document", service.getFullName()));

                    IDataCursor cursor = pipeline.getCursor();
                    try {
                        IDataHelper.put(cursor, "$task", job);
                        if (bizdoc != null) {
                            IDataHelper.put(cursor, "bizdoc", BizDocEnvelopeHelper.normalize(bizdoc, true));
                            IDataHelper.put(cursor, "sender", ProfileCache.getInstance().get(bizdoc.getSenderId()));
                            IDataHelper.put(cursor, "receiver", ProfileCache.getInstance().get(bizdoc.getReceiverId()));
                        }
                    } finally {
                        cursor.destroy();
                    }

                    ServiceThread serviceThread = Service.doThreadInvoke(service, session, pipeline);
                    output = serviceThread.getIData();

                    owningThread.setName(MessageFormat.format("{0}: Task {1} COMPLETED {2}", owningThreadPrefix, jobLogString, DateTimeHelper.now("datetime")));
                }
            } catch (Exception ex) {
                owningThread.setName(MessageFormat.format("{0}: Task {1} FAILED: {2} {3}", owningThreadPrefix, jobLogString, ExceptionHelper.getMessage(ex), DateTimeHelper.now("datetime")));
                exception = ex;
            } finally {
                owningThread.setName(owningThreadPrefix);
                if (requiresCompletion) {
                    setJobCompleted(output, exception, System.nanoTime() - startTime);
                }
                if (exception != null) {
                    throw exception;
                }
            }
        }

        return output;
    }

    /**
     * Sets the job as either successfully or unsuccessfully completed, depending on whether
     * and exception is provided.
     *
     * @param serviceOutput The output of the service used to process the job.
     * @param exception     Optional exception encountered while processing the job.
     * @param duration      The time taken to process the job in milliseconds.
     * @throws Exception    If a database error occurs.
     */
    private void setJobCompleted(IData serviceOutput, Throwable exception, long duration) throws Exception {
        boolean success = exception == null;
        int retry = 1;

        continuousFailureDetector.didComplete(success);

        while(true) {
            try {
                job.setTransportTime(duration/1000000L);
                job.setOutputData(serviceOutput);

                if (success) {
                    job.setTransportStatus("success");
                } else {
                    job.setTransportStatus("fail");
                    job.setTransportStatusMessage(StringHelper.truncate(ExceptionHelper.getMessage(exception), TRANSPORT_STATUS_MESSAGE_LENGTH, true));

                    if (retryLimit > 0 && GuaranteedJobHelper.hasUnrecoverableErrors(job)) {
                        // abort the delivery job so it won't be retried
                        GuaranteedJobHelper.setRetryStrategy(job, 0, 1, 0);
                        GuaranteedJobHelper.log(job, EntryType.ERROR, "Delivery", "Delivery aborted", MessageFormat.format("Delivery task {0} on {1} queue {2} was aborted due to unrecoverable errors being encountered, and will not be retried", job.getJobId(), queue.getQueueType(), queue.getQueueName()));
                    } else {
                        GuaranteedJobHelper.setRetryStrategy(job, retryLimit, retryFactor, timeToWait);
                    }
                }

                if (job instanceof DeliveryJob) {
                    job = new RetryableDeliveryJob((DeliveryJob)job, suspend, exhaustedStatus);
                }

                QueuingUtils.updateStatus(job, success);

                if (job instanceof RetryableDeliveryJob) {
                    job = ((RetryableDeliveryJob)job).getDelegate();
                }

                break;
            } catch(Exception ex) {
                if (++retry > MAX_RETRIES) {
                    throw ex;
                } else {
                    try {
                        Thread.sleep(WAIT_BETWEEN_RETRIES_MILLISECONDS);
                    } catch(InterruptedException interruption) {
                        break;
                    }
                }
            }
        }

        List<NSService> stack = ServiceHelper.getCallStack();
        stack.add((NSService)Namespace.current().getNode(service));

        User currentUser = InvokeState.getCurrentState().getUser();
        String functionPrefix;
        if (currentUser == null) {
            functionPrefix = "";
        } else {
            functionPrefix = currentUser.getName() + " -- ";
        }

        ServerLogger.info(functionPrefix + IterableHelper.join(stack, " → "),"{0} queue processed task {1} -- {2} -- {3}", queue.getQueueName(), GuaranteedJobHelper.toLogString(job), success ? "COMPLETED" : "FAILED: " + ExceptionHelper.getMessage(exception), DurationHelper.format(duration, DurationPattern.NANOSECONDS, DurationPattern.XML));
    }
}
