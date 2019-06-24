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
import com.wm.app.tn.delivery.GuaranteedJob;
import com.wm.app.tn.delivery.JobMgr;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.app.tn.manage.OmiUtils;
import com.wm.app.tn.profile.ProfileStore;
import com.wm.app.tn.profile.ProfileStoreException;
import com.wm.app.tn.profile.ProfileSummary;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataUtil;
import permafrost.tundra.tn.document.BizDocEnvelopeHelper;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.xml.datatype.Duration;

/**
 * A collection of convenience methods for working with Trading Networks delivery jobs.
 */
public final class GuaranteedJobHelper {
    /**
     * SQL statement for updating a Trading Networks delivery job.
     */
    private static final String UPDATE_DELIVERY_JOB_SQL = "UPDATE DeliveryJob SET TimeCreated = ?, TimeUpdated = ?, JobStatus = ?, Retries = ?, TransportStatus = ?, TransportStatusMessage = ?, TransportTime = ?, OutputData = ?, ServerID = ?, TypeData = ?, QueueName = ?, UserName = ? WHERE JobID = ?";
    /**
     * SQL statement for updating the retry strategy of a Trading Networks delivery job.
     */
    private static final String UPDATE_DELIVERY_JOB_RETRY_STRATEGY_SQL = "UPDATE DeliveryJob SET RetryLimit = ?, RetryFactor = ?, TimeToWait = ? WHERE JobID = ?";
    /**
     * SQL statement for updating a Trading Networks delivery job status to "DELIVERING".
     */
    private static final String UPDATE_DELIVERY_JOB_STATUS_TO_DELIVERING_SQL = "deliver.job.update.delivering";
    /**
     * SQL statement for selecting all Trading Networks delivery jobs for a specific bizdoc.
     */
    private static final String SELECT_DELIVERY_JOBS_FOR_BIZDOC_SQL = "delivery.jobid.select.docid";
    /**
     * The default timeout for database queries.
     */
    private static final int DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS = 30;
    /**
     * The number of decimal places expected in fixed decimal point retry factor.
     */
    public static final int RETRY_FACTOR_DECIMAL_PRECISION = 3;
    /**
     * The multiplier to use to pack a decimal retry factor into an int.
     */
    public static final float RETRY_FACTOR_DECIMAL_MULTIPLIER = (float)Math.pow(10, RETRY_FACTOR_DECIMAL_PRECISION);

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
     * Returns a GuaranteedJob, if given either a subset or full GuaranteedJob as an IData document.
     *
     * @param input             An IData document which could be a GuaranteedJob, or could be a subset of a
     *                          GuaranteedJob that includes an TaskId key.
     * @return                  The GuaranteedJob associated with the given IData document.
     */
    public static GuaranteedJob normalize(IData input) {
        if (input == null) return null;

        GuaranteedJob job = null;

        if (input instanceof GuaranteedJob) {
            job = (GuaranteedJob)input;
        } else {
            IDataCursor cursor = input.getCursor();
            String id = IDataUtil.getString(cursor, "TaskId");
            cursor.destroy();

            if (id == null) throw new IllegalArgumentException("TaskId is required");

            job = get(id);
        }

        return job;
    }

    /**
     * Returns all delivery queue jobs associated with the given BizDocEnvelope.
     *
     * @param bizdoc        The BizDocEnvelope to return all associated jobs for.
     * @return              An array of all delivery queue jobs associated with the given BizDocEnvelope.
     * @throws SQLException If a database error occurs.
     */
    public static GuaranteedJob[] list(BizDocEnvelope bizdoc) throws SQLException {
        if (bizdoc == null) return null;

        String[] taskIDs = list(bizdoc.getInternalId());

        List<GuaranteedJob> output = new ArrayList<GuaranteedJob>();

        for (String taskID : taskIDs) {
            output.add(get(taskID));
        }

        return output.toArray(new GuaranteedJob[0]);
    }

    /**
     * Returns all delivery queue jobs associated with the given BizDocEnvelope.
     *
     * @param internalID    The internal ID of the BizDocEnvelope to return all associated jobs for.
     * @return              An array of all delivery queue job IDs associated with the given BizDocEnvelope.
     * @throws SQLException If a database error occurs.
     */
    public static String[] list(String internalID) throws SQLException {
        if (internalID == null) return null;

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        List<String> output = new ArrayList<String>();

        try {
            connection = Datastore.getConnection();
            statement = SQLStatements.prepareStatement(connection, SELECT_DELIVERY_JOBS_FOR_BIZDOC_SQL);
            statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
            statement.clearParameters();

            statement.setString(1, internalID);

            resultSet = statement.executeQuery();
            while(resultSet.next()) {
                output.add(resultSet.getString(1));
            }

            connection.commit();
        } catch (SQLException ex) {
            connection = Datastore.handleSQLException(connection, ex);
            throw ex;
        } finally {
            SQLWrappers.close(resultSet);
            SQLStatements.releaseStatement(statement);
            Datastore.releaseConnection(connection);
        }

        return output.toArray(new String[0]);
    }

    /**
     * Restarts the given job. This method, unlike JobMgr.restartJob, does not require the job status to be
     * "STOPPED" or "FAILED", and will restart the given job regardless of its status.
     *
     * @param job The job to be restarted.
     */
    public static void restart(GuaranteedJob job) {
        if (job != null) {
            job.reset();
            job.save();
        }
    }

    /**
     * Update the retry settings on the given job using the given settings, or the retry settings on the receiver's
     * profile if the given retryLimit is less than or equal to 0.
     *
     * @param job                       The job to be updated.
     * @param retryLimit                The number of retries this job should attempt.
     * @param retryFactor               The factor used to extend the time to wait on each retry.
     * @param timeToWait                The time to wait between each retry.
     * @throws ProfileStoreException    If a database error is encountered.
     * @throws SQLException             If a database error is encountered.
     */
    public static void setRetryStrategy(GuaranteedJob job, int retryLimit, float retryFactor, Duration timeToWait) throws ProfileStoreException, SQLException {
        setRetryStrategy(job, retryLimit, retryFactor, timeToWait == null ? 0 : timeToWait.getTimeInMillis(new Date()));
    }

    /**
     * Update the retry settings on the given job using the given settings, or the retry settings on the receiver's
     * profile if the given retryLimit is less than or equal to 0.
     *
     * @param job                       The job to be updated.
     * @param retryLimit                The number of retries this job should attempt.
     * @param retryFactor               The factor used to extend the time to wait on each retry.
     * @param timeToWait                The time in milliseconds to wait between each retry.
     * @throws ProfileStoreException    If a database error is encountered.
     * @throws SQLException             If a database error is encountered.
     */
    public static void setRetryStrategy(GuaranteedJob job, int retryLimit, float retryFactor, long timeToWait) throws ProfileStoreException, SQLException {
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

                if (retryFactor >= RETRY_FACTOR_DECIMAL_MULTIPLIER || retryFactor % 1 != 0) {
                    // if retry factor has decimal precision, pack it into an integer by multiplying with a factor
                    // which preserves the configured precision
                    job.setRetryFactor(Math.round(retryFactor * RETRY_FACTOR_DECIMAL_MULTIPLIER));
                } else {
                    job.setRetryFactor(Math.round(retryFactor));
                }
                job.setTTW(timeToWait);

                connection = Datastore.getConnection();
                statement = connection.prepareStatement(UPDATE_DELIVERY_JOB_RETRY_STRATEGY_SQL);
                statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
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
            throw ex;
        } finally {
            SQLWrappers.close(statement);
            Datastore.releaseConnection(connection);
        }
    }

    /**
     * Update the given job's status to "DELIVERING".
     *
     * @param job               The job to be updated.
     * @throws SQLException     If a database error is encountered.
     */
    protected static void setDelivering(GuaranteedJob job) throws SQLException {
        if (job == null) return;

        Connection connection = null;
        PreparedStatement statement = null;

        try {
            connection = Datastore.getConnection();

            statement = SQLStatements.prepareStatement(connection, UPDATE_DELIVERY_JOB_STATUS_TO_DELIVERING_SQL);
            statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
            statement.clearParameters();

            SQLWrappers.setChoppedString(statement, 1, JobMgr.getJobMgr().getServerId(), "DeliveryJob.ServerID");
            SQLWrappers.setCharString(statement, 2, job.getJobId());

            int rowCount = statement.executeUpdate();
            connection.commit();

            if (rowCount == 1) job.delivering();
        } catch (SQLException ex) {
            connection = Datastore.handleSQLException(connection, ex);
            throw ex;
        } finally {
            SQLStatements.releaseStatement(statement);
            Datastore.releaseConnection(connection);
        }
    }

    /**
     * Saves the given job to the Trading Networks database. This method differs from job.save() as this method
     * preserves the job's updated time rather than setting it to current time as job.save() does, and supports
     * updating the job's created time if it has changed.
     *
     * @param job           The job to be saved.
     * @throws IOException  If an I/O error is encountered.
     * @throws SQLException If a database error is encountered.
     */
    public static void save(GuaranteedJob job) throws IOException, SQLException {
        if (job == null) return;

        Connection connection = null;
        PreparedStatement statement = null;

        try {
            connection = Datastore.getConnection();
            statement = connection.prepareStatement(UPDATE_DELIVERY_JOB_SQL);
            statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
            statement.clearParameters();

            SQLWrappers.setTimestamp(statement, 1, new Timestamp(job.getTimeCreated()));
            SQLWrappers.setTimestamp(statement, 2, new Timestamp(job.getTimeUpdated()));
            SQLWrappers.setChoppedString(statement, 3, job.getStatus(), "DeliveryJob.JobStatus");
            statement.setInt(4, job.getRetries());
            SQLWrappers.setChoppedString(statement, 5, job.getTransportStatus(), "DeliveryJob.TransportStatus");
            SQLWrappers.setChoppedString(statement, 6, job.getTransportStatusMessage(), "DeliveryJob.TransportStatusMessage");
            statement.setInt(7, (int)job.getTransportTime());
            SQLWrappers.setBinaryStream(statement, 8, job.getOutputData());
            SQLWrappers.setChoppedString(statement, 9, job.getServerId(), "DeliveryJob.ServerID");
            SQLWrappers.setBinaryStream(statement, 10, job.getDBIData());
            SQLWrappers.setChoppedString(statement, 11, job.getQueueName(), "DeliveryQueue.QueueName");
            SQLWrappers.setChoppedString(statement, 12, job.getInvokeAsUser(), "DeliveryJob.UserName");
            SQLWrappers.setCharString(statement, 13, job.getJobId());

            statement.executeUpdate();
            connection.commit();
        } catch (SQLException ex) {
            connection = Datastore.handleSQLException(connection, ex);
            throw ex;
        } finally {
            SQLStatements.releaseStatement(statement);
            Datastore.releaseConnection(connection);
        }
    }

    /**
     * Returns true if the owning BizDocEnvelope for the given GuaranteedJob has any unrecoverable errors.
     *
     * @param job The GuaranteedJob to check for unrecoverable errors.
     * @return    True if the given GuaranteedJob has unrecoverable errors.
     * @throws ServiceException If a database error occurs.
     */
    public static boolean hasUnrecoverableErrors(GuaranteedJob job) throws ServiceException {
        return BizDocEnvelopeHelper.hasUnrecoverableErrors(job.getBizDocEnvelope());
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

    /**
     * Returns a string that can be used to log the given job.
     *
     * @param job   The job to be logged.
     * @return      A string representing the given job.
     */
    public static String toLogString(GuaranteedJob job) {
        String output;

        BizDocEnvelope bizdoc = job.getBizDocEnvelope();
        if (bizdoc == null) {
            output = MessageFormat.format("'{'ID={0}'}'", job.getJobId());
        } else {
            output = MessageFormat.format("'{'ID={0}, BizDoc={1}'}'", job.getJobId(), BizDocEnvelopeHelper.toLogString(bizdoc));
        }

        return output;
    }
}
