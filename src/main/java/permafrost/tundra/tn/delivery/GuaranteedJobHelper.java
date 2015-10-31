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

import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.db.Datastore;
import com.wm.app.tn.db.DeliveryStore;
import com.wm.app.tn.db.SQLStatements;
import com.wm.app.tn.db.SQLWrappers;
import com.wm.app.tn.delivery.DeliveryQueue;
import com.wm.app.tn.delivery.GuaranteedJob;
import com.wm.app.tn.delivery.JobMgr;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.app.tn.manage.OmiUtils;
import com.wm.app.tn.profile.ProfileStore;
import com.wm.app.tn.profile.ProfileSummary;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.time.DateTimeHelper;
import permafrost.tundra.tn.document.BizDocEnvelopeHelper;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A collection of convenience methods for working with Trading Networks delivery jobs.
 */
public class GuaranteedJobHelper {
    /**
     * SQL statement for updating a Trading Networks delivery job.
     */
    private static final String UPDATE_DELIVERY_JOB_SQL = "deliver.job.update";

    /**
     * SQL statement for updating the retry strategy of a Trading Networks delivery job.
     */
    private static final String UPDATE_DELIVERY_JOB_RETRY_STRATEGY_SQL = "UPDATE DeliveryJob SET RetryLimit = ?, RetryFactor = ?, TimeToWait = ? WHERE JobID = ?";

    /**
     * SQL statement for updating a Trading Networks delivery job status to "DELIVERING"
     */
    private static final String UPDATE_DELIVERY_JOB_STATUS_TO_DELIVERING_SQL = "deliver.job.update.delivering";

    /**
     * Disallow instantiation of this class.
     */
    private GuaranteedJobHelper() {}

    /**
     * Returns the job with the given ID.
     *
     * @param id The ID of the job to be returned.
     * @return   The job associated with the given ID.
     */
    public static GuaranteedJob get(String id) {
        if (id == null) return null;
        return DeliveryStore.getAnyJob(id, OmiUtils.isOmiEnabled());
    }

    /**
     * Returns the given job, refreshed from the Trading Networks database.
     *
     * @param job The job to be refreshed.
     * @return    The given job, refreshed from the Trading Networks database.
     */
    public static GuaranteedJob refresh(GuaranteedJob job) {
        if (job == null) return null;
        return get(job.getJobId());
    }

    /**
     * Update the retry settings on the given job using the given settings, or the retry settings on the receiver's profile
     * if the given retryLimit <= 0.
     *
     * @param job               The job to be updated.
     * @param retryLimit        The number of retries this job should attempt.
     * @param retryFactor       The factor used to extend the time to wait on each retry.
     * @param timeToWait        The time in seconds to wait between each retry.
     * @throws ServiceException If a database error is encountered.
     */
    public static void setRetryStrategy(GuaranteedJob job, int retryLimit, int retryFactor, int timeToWait) throws ServiceException {
        if (job == null) return;

        Connection connection = null;
        PreparedStatement statement = null;

        try {
            int taskRetryLimit = job.getRetryLimit();
            int taskRetryFactor = job.getRetryFactor();
            int taskTTW = (int)job.getTTW();

            BizDocEnvelope bizdoc = job.getBizDocEnvelope();
            ProfileSummary receiver = ProfileStore.getProfileSummary(bizdoc.getReceiverId());

            if (retryLimit <= 0 && receiver.getDeliveryRetries() > 0) {
                retryLimit = receiver.getDeliveryRetries();
                retryFactor = receiver.getRetryFactor();
                timeToWait = receiver.getDeliveryWait();
            }

            if (taskRetryLimit != retryLimit || taskRetryFactor != retryFactor || taskTTW != timeToWait) {
                job.setRetryLimit(retryLimit);
                job.setRetryFactor(retryFactor);
                job.setTTW(timeToWait);

                connection = Datastore.getConnection();
                statement = connection.prepareStatement(UPDATE_DELIVERY_JOB_RETRY_STRATEGY_SQL);
                statement.clearParameters();

                statement.setInt(1, job.getRetryLimit());
                statement.setInt(2, job.getRetryFactor());
                statement.setInt(3, (int)job.getTTW());
                SQLWrappers.setCharString(statement, 4, job.getJobId());

                statement.executeUpdate();
                connection.commit();
            }
        } catch (SQLException ex) {
            connection = Datastore.handleSQLException(connection, ex);
            ExceptionHelper.raise(ex);
        } finally {
            SQLWrappers.close(statement);
            Datastore.releaseConnection(connection);
        }
    }

    /**
     * Update the given job's status to "DELIVERING".
     *
     * @param job               The job to be updated.
     * @throws ServiceException If a database error is encountered.
     */
    protected static void setDelivering(GuaranteedJob job) throws ServiceException {
        if (job == null) return;

        Connection connection = null;
        PreparedStatement statement = null;

        try {
            connection = Datastore.getConnection();
            statement = SQLStatements.prepareStatement(connection, UPDATE_DELIVERY_JOB_STATUS_TO_DELIVERING_SQL);
            statement.clearParameters();

            SQLWrappers.setChoppedString(statement, 1, JobMgr.getJobMgr().getServerId(), "DeliveryJob.ServerID");
            SQLWrappers.setCharString(statement, 2, job.getJobId());

            statement.executeUpdate();
            job.delivering();
            connection.commit();
        } catch (SQLException ex) {
            connection = Datastore.handleSQLException(connection, ex);
            ExceptionHelper.raise(ex);
        } finally {
            SQLStatements.releaseStatement(statement);
            Datastore.releaseConnection(connection);
        }
    }

    /**
     * Saves the given job to the Trading Networks database.
     *
     * @param job The job to be saved.
     * @throws ServiceException If a database error is encountered.
     */
    protected static void save(GuaranteedJob job) throws ServiceException {
        if (job == null) return;

        Connection connection = null;
        PreparedStatement statement = null;

        try {
            connection = Datastore.getConnection();
            statement = SQLStatements.prepareStatement(connection, UPDATE_DELIVERY_JOB_SQL);
            statement.clearParameters();

            // instead of setting TimeUpdated to now, set it to the time in the job object
            SQLWrappers.setTimestamp(statement, 1, new java.sql.Timestamp(job.getTimeUpdated()));

            SQLWrappers.setChoppedString(statement, 2, job.getStatus(), "DeliveryJob.JobStatus");
            statement.setInt(3, job.getRetries());
            SQLWrappers.setChoppedString(statement, 4, job.getTransportStatus(), "DeliveryJob.TransportStatus");
            SQLWrappers.setChoppedString(statement, 5, job.getTransportStatusMessage(), "DeliveryJob.TransportStatusMessage");
            statement.setInt(6, (int)job.getTransportTime());
            SQLWrappers.setBinaryStream(statement, 7, job.getOutputData());
            SQLWrappers.setChoppedString(statement, 8, job.getServerId(), "DeliveryJob.ServerID");
            SQLWrappers.setBinaryStream(statement, 9, job.getDBIData());
            SQLWrappers.setChoppedString(statement, 10, job.getQueueName(), "DeliveryQueue.QueueName");
            SQLWrappers.setChoppedString(statement, 11, job.getInvokeAsUser(), "DeliveryJob.UserName");
            SQLWrappers.setCharString(statement, 12, job.getJobId());

            statement.executeUpdate();
            connection.commit();
        } catch (SQLException ex) {
            connection = Datastore.handleSQLException(connection, ex);
            ExceptionHelper.raise(ex);
        } catch (IOException ex) {
            ExceptionHelper.raise(ex);
        } finally {
            SQLStatements.releaseStatement(statement);
            Datastore.releaseConnection(connection);
        }
    }

    /**
     * Re-enqueues the given job for delivery, unless it has reached its retry limit.
     *
     * @param job     The job to be retried.
     * @param suspend Whether the owning delivery queue should be suspended if the job has
     *                reached its retry limit.
     * @throws ServiceException If a database error is encountered.
     */
    public static void retry(GuaranteedJob job, boolean suspend) throws ServiceException {
        if (job == null) return;

        job = refresh(job);
        //BizDocEnvelope bizdoc = job.getBizDocEnvelope();

        int retryLimit = job.getRetryLimit();
        int retries = job.getRetries();
        String status = job.getStatus();
        String queueName = job.getQueueName();

        DeliveryQueue queue = DeliveryQueueHelper.get(queueName);

        boolean exhausted = retries >= retryLimit && status.equals("FAILED");
        boolean failed = (retries > 0 && status.equals("QUEUED")) || exhausted;

        if (failed) {
            if (exhausted) {
                if (retryLimit > 0) {
                    log(job, "ERROR", "Delivery", java.text.MessageFormat.format("Exhausted all retries ({0}/{1})", retries, retryLimit), java.text.MessageFormat.format("Exhausted all retries ({0} of {1}) of delivery task ''{2}''", retries, retryLimit, job.getJobId()));
                }

                if (suspend) {
                    // reset retries back to 1
                    retries = 1;
                    job.setRetries(retries);
                    job.setStatus(GuaranteedJob.QUEUED);
                    job.setDefaultServerId();

                    long nextRetry = calculateNextRetryDateTime(job);
                    job.setTimeUpdated(nextRetry);
                    save(job);

                    boolean isSuspended = queue.isSuspended();

                    if (!isSuspended) {
                        // suspend the queue if not already suspended
                        DeliveryQueueHelper.suspend(queue);

                        if (queue.getQueueType().equals("private")) {
                            log(job, "WARNING", "Delivery", "Suspended receiver's private queue '" + queueName + "'", "Delivery of receiver's private queue '" + queueName + "' was suspended due to task exhaustion");
                        } else {
                            log(job, "WARNING", "Delivery", "Suspended public queue '" + queueName + "'", "Delivery of public queue '" + queueName + "' was suspended due to task exhaustion");
                        }
                    }

                    BizDocEnvelopeHelper.setStatus(job.getBizDocEnvelope(), "QUEUED", isSuspended ? "REQUEUED" : "SUSPENDED");
                    log(job, "MESSAGE", "Delivery", java.text.MessageFormat.format("Retries reset ({0}/{1})", retries, retryLimit), java.text.MessageFormat.format("Retries reset to ensure task is processed upon queue delivery resumption; if this task is not required to be processed again, it should be manually deleted. Next retry ({0} of {1}) scheduled no earlier than ''{2}''", retries, retryLimit, DateTimeHelper.format(nextRetry)));
                }
            } else {
                long nextRetry = calculateNextRetryDateTime(job);
                job.setTimeUpdated(nextRetry); // force this job to wait for its next retry
                save(job);

                BizDocEnvelopeHelper.setStatus(job.getBizDocEnvelope(), "QUEUED", "REQUEUED");
                log(job, "MESSAGE", "Delivery", java.text.MessageFormat.format("Next retry scheduled ({0}/{1})", retries, retryLimit), java.text.MessageFormat.format("Next retry ({0} of {1}) scheduled no earlier than ''{2}''", retries, retryLimit, DateTimeHelper.format(nextRetry)));
            }
        }
    }

    /**
     * Calculates the next time the given job should be retried according to its retry settings.
     *
     * @param job The job to be retried.
     * @return    The datetime, as the number of milliseconds since the epoch, at which the job should next be retried.
     */
    private static long calculateNextRetryDateTime(GuaranteedJob job) {
        long now = new java.util.Date().getTime();
        long nextRetry = now;

        int retryCount = job.getRetries();
        int retryFactor = job.getRetryFactor();
        int ttw = (int)job.getTTW();

        if (ttw > 0) {
            if (retryFactor > 1 && retryCount > 1) {
                nextRetry = now + (long)(ttw * Math.pow(retryFactor, retryCount - 1));
            } else {
                nextRetry = now + ttw;
            }
        }

        return nextRetry;
    }

    /**
     * Adds an activity log statement to the given job.
     *
     * @param job     The GuaranteedJob to add the activity log statement to.
     * @param type    The type of message to be logged.
     * @param klass   The class of the message to be logged.
     * @param summary The summary of the message to be logged.
     * @param message The detail of the message to be logged.
     * @throws ServiceException If an error occurs while logging.
     */
    public static void log(GuaranteedJob job, String type, String klass, String summary, String message) throws ServiceException {
        BizDocEnvelopeHelper.log(job.getBizDocEnvelope(), type, klass, summary, message);
    }

}
