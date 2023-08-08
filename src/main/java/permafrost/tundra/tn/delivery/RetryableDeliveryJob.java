package permafrost.tundra.tn.delivery;

import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.Session;
import com.wm.app.tn.delivery.DeliveryJob;
import com.wm.app.tn.delivery.DeliveryQueue;
import com.wm.app.tn.delivery.GuaranteedJob;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.lang.ns.NSName;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.math.BigDecimalHelper;
import permafrost.tundra.math.RoundingModeHelper;
import permafrost.tundra.time.DateTimeHelper;
import permafrost.tundra.tn.document.BizDocEnvelopeHelper;
import permafrost.tundra.tn.log.EntryType;
import permafrost.tundra.tn.cache.ProfileCache;
import java.math.BigDecimal;
import java.text.MessageFormat;
import static permafrost.tundra.tn.delivery.GuaranteedJobHelper.log;

/**
 * A DeliveryJob which delegates to another DeliveryJob, but also supports taking into account the job's retry
 * settings when saved after failure by using TimeUpdated to schedule the next retry at the appropriate time in
 * the future.
 */
public class RetryableDeliveryJob extends DelegatingDeliveryJob {
    /**
     * The system status to use when queuing a BizDocEnvelope.
     */
    private static final String BIZDOC_ENVELOPE_QUEUED_SYSTEM_STATUS = "QUEUED";
    /**
     * The user status to use when retrying a BizDocEnvelope delivery queue job.
     */
    private static final String BIZDOC_ENVELOPE_REQUEUED_USER_STATUS = "REQUEUED";
    /**
     * The user status to use when suspending a delivery queue due to BizDocEnvelope delivery queue job exhaustion.
     */
    private static final String BIZDOC_ENVELOPE_SUSPENDED_USER_STATUS = "SUSPENDED";
    /**
     * The user status to use when a BizDocEnvelope's queued job is exhausted.
     */
    private static final String BIZDOC_ENVELOPE_EXHAUSTED_USER_STATUS = "EXHAUSTED";
    /**
     * Whether the deliver queue should be suspended on retry exhaustion.
     */
    protected boolean suspend;
    /**
     * The user status a BizDocEnvelope is set to if all deliveries of the job are exhausted.
     */
    protected String exhaustedStatus;
    /**
     * Optional service to be invoked when/if all retries are exhausted.
     */
    protected NSName exhaustedService;
    /**
     * Optional session used when invoking the exhausted service.
     */
    protected Session exhaustedSession;
    /**
     * Optional pipeline used when invoking the exhausted service.
     */
    protected IData exhaustedPipeline;

    /**
     * Creates a new RetryableDeliveryJob.
     *
     * @param delegate          The delivery job this job delegates to.
     * @param suspend           Whether to suspend the delivery queue on job retry exhaustion.
     * @param exhaustedStatus   The status set on the related bizdoc when all retries of the job are exhausted.
     * @param exhaustedService  Optional service to be invoked when all retries are exhausted.
     * @param exhaustedSession  Optional session to use when invoking the exhausted service.
     * @param exhaustedPipeline Optional pipeline used when invoking the exhausted service.
     */
    public RetryableDeliveryJob(DeliveryJob delegate, boolean suspend, String exhaustedStatus, NSName exhaustedService, Session exhaustedSession, IData exhaustedPipeline) {
        super(delegate);
        this.suspend = suspend;
        if (exhaustedStatus == null) {
            this.exhaustedStatus = BIZDOC_ENVELOPE_EXHAUSTED_USER_STATUS;
        } else {
            this.exhaustedStatus = exhaustedStatus;
        }
        this.exhaustedService = exhaustedService;
        this.exhaustedSession = exhaustedSession;
        this.exhaustedPipeline = exhaustedPipeline == null ? IDataFactory.create() : exhaustedPipeline;
    }

    /**
     * Saves this job, taking into account if it needs to be retried and if so updating the TimeUpdated to be
     * the next time the job should be retried.
     *
     * @return True if the save was successful.
     */
    @Override
    public boolean save() {
        boolean result;
        boolean exhausted = false;
        long nextRetry = -1, timeCreated = getTimeCreated();
        int retryLimit = getRetryLimit();
        int retries = getRetries();
        int status = getStatusVal();
        String queueName = getQueueName();

        try {
            DeliveryQueue queue = DeliveryQueueHelper.get(queueName);

            boolean statusSilence = DeliveryQueueHelper.getStatusSilence(queue);
            exhausted = retries >= retryLimit && status == GuaranteedJob.FAILED;
            boolean failed = (retries > 0 && status == GuaranteedJob.QUEUED) || exhausted;

            if (failed) {
                if (exhausted) {
                    if (retryLimit > 0) {
                        BizDocEnvelopeHelper.setStatus(getBizDocEnvelope(), null, exhaustedStatus, statusSilence);
                        log(this, EntryType.ERROR, "Delivery", MessageFormat.format("Exhausted all retries ({0}/{1})", retries, retryLimit), MessageFormat.format("Exhausted all retries ({0} of {1}) of task {2} on {3} queue {4}", retries, retryLimit, getJobId(), queue.getQueueType(), queueName));
                    }

                    if (suspend) {
                        // reset retries back to 1
                        retries = 1;
                        setRetries(retries);
                        setStatus(GuaranteedJob.QUEUED);
                        setDefaultServerId();

                        nextRetry = calculateNextRetryDateTime();
                        setTimeCreated(nextRetry);
                        GuaranteedJobHelper.save(this);

                        boolean isSuspended = queue.isSuspended();

                        if (!isSuspended) {
                            // suspend the queue if not already suspended
                            DeliveryQueueHelper.suspend(queue);
                            log(this, EntryType.WARNING, "Delivery", MessageFormat.format("Suspended {0} queue {1}", queue.getQueueType(), queueName), MessageFormat.format("Delivery of {0} queue {1} was suspended due to task {2} exhaustion", queue.getQueueType(), queueName, getJobId()));
                        }

                        BizDocEnvelopeHelper.setStatus(getBizDocEnvelope(), BIZDOC_ENVELOPE_QUEUED_SYSTEM_STATUS, isSuspended ? BIZDOC_ENVELOPE_REQUEUED_USER_STATUS : BIZDOC_ENVELOPE_SUSPENDED_USER_STATUS, statusSilence);
                        log(this, EntryType.MESSAGE, "Delivery", MessageFormat.format("Retries reset ({0}/{1})", retries, retryLimit), MessageFormat.format("Retries reset to ensure task is processed upon queue delivery resumption; if this task is not required to be processed again, it should be manually deleted. Next retry ({0} of {1}) of task {2} on {3} queue {4} scheduled no earlier than {5}", retries, retryLimit, getJobId(), queue.getQueueType(), queueName, DateTimeHelper.format(nextRetry)));
                    }
                } else {
                    nextRetry = calculateNextRetryDateTime();
                    setTimeCreated(nextRetry);
                    GuaranteedJobHelper.save(this);

                    BizDocEnvelopeHelper.setStatus(getBizDocEnvelope(), BIZDOC_ENVELOPE_QUEUED_SYSTEM_STATUS, BIZDOC_ENVELOPE_REQUEUED_USER_STATUS, statusSilence);
                    log(this, EntryType.MESSAGE, "Delivery", MessageFormat.format("Next retry scheduled ({0}/{1})", retries, retryLimit), MessageFormat.format("Next retry ({0} of {1}) of task {2} on {3} queue {4} scheduled no earlier than {5}", retries, retryLimit, getJobId(), queue.getQueueType(), queueName, DateTimeHelper.format(nextRetry)));
                }
            }

            // call the standard save method, note that this sets the time updated to current time,
            // which is why we previously updated time created to be the next retry time temporarily
            result = getDelegate().save();
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            if (nextRetry >= 0) {
                try {
                    // restore time created, and now set time updated to the next retry time
                    setTimeCreated(timeCreated);
                    setTimeUpdated(nextRetry);
                    GuaranteedJobHelper.save(this);
                } catch(Exception ex) {
                    // suppress this exception
                }
            }

            if (exhausted && exhaustedService != null) {
                exhaustedPipeline = IDataHelper.merge(exhaustedPipeline, this.getOutputData());
                IDataCursor cursor = exhaustedPipeline.getCursor();
                try {
                    BizDocEnvelope bizdoc = this.getBizDocEnvelope();
                    IDataHelper.put(cursor, "$task", this);
                    if (bizdoc != null) {
                        bizdoc = BizDocEnvelopeHelper.normalize(bizdoc, true);
                        IData sender = ProfileCache.getInstance().get(bizdoc.getSenderId());
                        IData receiver = ProfileCache.getInstance().get(bizdoc.getReceiverId());
                        IDataHelper.put(cursor, "$bizdoc", bizdoc);
                        IDataHelper.put(cursor, "$sender", sender);
                        IDataHelper.put(cursor, "$receiver", receiver);
                        IDataHelper.put(cursor, "bizdoc", bizdoc);
                        IDataHelper.put(cursor, "sender", sender);
                        IDataHelper.put(cursor, "receiver", receiver);
                    }

                    GuaranteedJobHelper.log(this, EntryType.MESSAGE, "Delivery", "Exhausted processing attempt", MessageFormat.format("Exhausted service {0} attempting to process document", exhaustedService.getFullName()));
                    Service.doInvoke(exhaustedService, exhaustedSession, exhaustedPipeline);
                    GuaranteedJobHelper.log(this, EntryType.MESSAGE, "Delivery", "Exhausted processing successful", MessageFormat.format("Exhausted service {0} processed document successfully", exhaustedService.getFullName()));
                } catch(Exception ex) {
                    GuaranteedJobHelper.log(this, EntryType.ERROR, "Delivery", MessageFormat.format("Exhausted processing failed: {0}", ExceptionHelper.getMessage(ex, true)), ExceptionHelper.getStackTraceString(ex, 3));
                } finally {
                    cursor.destroy();
                }
            }
        }

        return result;
    }

    /**
     * Calculates the next time this job should be retried according to its retry settings.
     *
     * @return The datetime, as the number of milliseconds since the epoch, at which the job should next be retried.
     */
    private long calculateNextRetryDateTime() {
        long now = System.currentTimeMillis();
        long nextRetry = now;

        int retryCount = getRetries();
        float retryFactor = getRetryFactor();
        int ttw = (int)getTTW();

        if (ttw > 0) {
            if (retryFactor > 1.0f && retryCount > 1) {
                // if retryFactor is a packed decimal convert it to a fixed point decimal number (this is how we provide
                // support for non-integer retry factors)
                if (retryFactor >= GuaranteedJobHelper.RETRY_FACTOR_DECIMAL_MULTIPLIER) {
                    retryFactor = BigDecimalHelper.round(new BigDecimal(retryFactor / GuaranteedJobHelper.RETRY_FACTOR_DECIMAL_MULTIPLIER), GuaranteedJobHelper.RETRY_FACTOR_DECIMAL_PRECISION, RoundingModeHelper.DEFAULT_ROUNDING_MODE).floatValue();
                }

                nextRetry = now + (long)(ttw * Math.pow(retryFactor, retryCount - 1));
            } else {
                nextRetry = now + ttw;
            }
        }

        return nextRetry;
    }
}
