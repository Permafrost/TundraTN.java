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

import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.db.Datastore;
import com.wm.app.tn.db.QueueOperations;
import com.wm.app.tn.db.SQLWrappers;
import com.wm.app.tn.delivery.DeliveryQueue;
import com.wm.app.tn.delivery.DeliverySchedule;
import com.wm.app.tn.delivery.GuaranteedJob;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.lang.ns.NSName;
import com.wm.util.Masks;
import permafrost.tundra.data.IDataCursorHelper;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.lang.BooleanHelper;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.server.ScheduleHelper;
import javax.xml.datatype.Duration;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * A collection of convenience methods for working with Trading Networks delivery queues.
 */
public final class DeliveryQueueHelper {
    /**
     * SQL statement to select head of a delivery queue in job creation datetime order.
     */
    private static final String SELECT_NEXT_DELIVERY_JOB_ORDERED_SQL = "SELECT JobID FROM DeliveryJob WHERE JobStatus = 'QUEUED' AND QueueName = ? AND TimeCreated = (SELECT MIN(TimeCreated) FROM DeliveryJob WHERE JobStatus = 'QUEUED' AND QueueName = ?) AND TimeCreated <= ? AND TimeUpdated <= ? ORDER BY TimeCreated ASC, JobID ASC";
    /**
     * SQL statement to select head of a delivery queue in indeterminate order.
     */
    private static final String SELECT_NEXT_DELIVERY_JOB_UNORDERED_SQL = "SELECT JobID FROM DeliveryJob WHERE JobStatus = 'QUEUED' AND QueueName = ? AND TimeCreated <= ? AND TimeUpdated <= ? ORDER BY TimeCreated ASC, JobID ASC";
    /**
     * SQL statement to return the count of ordered queued jobs for a given queue.
     */
    private static final String SELECT_COUNT_QUEUED_JOBS_ORDERED_SQL = "SELECT COUNT(*) FROM DeliveryJob WHERE JobStatus = 'QUEUED' AND QueueName = ? AND TimeCreated = (SELECT MIN(TimeCreated) FROM DeliveryJob WHERE JobStatus = 'QUEUED' AND QueueName = ?) AND TimeCreated <= ? AND TimeUpdated <= ?";
    /**
     * SQL statement to return the count of unordered queued jobs for a given queue.
     */
    private static final String SELECT_COUNT_QUEUED_JOBS_UNORDERED_SQL = "SELECT COUNT(*) FROM DeliveryJob WHERE JobStatus = 'QUEUED' AND QueueName = ? AND TimeCreated <= ? AND TimeUpdated <= ?";
    /**
     * SQL statement to return which enabled or draining queues currently have pending QUEUED jobs.
     */
    private static final String SELECT_QUEUES_WITH_QUEUED_TASKS_SQL = "SELECT DISTINCT A.QueueName FROM DeliveryQueue A, DeliveryJob B WHERE A.QueueName = B.QueueName AND A.QueueState IN ('enabled', 'draining') AND B.JobStatus = 'QUEUED' AND B.TimeCreated <= ? AND B.TimeUpdated <= ?";
    /**
     * SQL statement to return the count of queues which are enabled or draining.
     */
    private static final String SELECT_COUNT_ENABLED_QUEUES_SQL = "SELECT COUNT(*) AS EnabledCount FROM DeliveryQueue WHERE QueueState IN ('enabled', 'draining')";
    /**
     * The age a delivery job must be before it is eligible to be processed.
     */
    private static final long DEFAULT_DELIVERY_JOB_AGE_THRESHOLD_MILLISECONDS = 0L;
    /**
     * The name of the service Trading Networks uses to execute delivery queue processing.
     */
    static final String DELIVER_BATCH_SERVICE_NAME = "wm.tn.queuing:deliverBatch";
    /**
     * The name of the service used to update a delivery queue.
     */
    private static final NSName UPDATE_QUEUE_SERVICE_NAME = NSName.create("wm.tn.queuing:updateQueue");
    /**
     * The default timeout for database queries.
     */
    private static final int DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS = 30;
    /**
     * The default maximum delivery interval.
     */
    private static final long DEFAULT_DELIVERY_INTERVAL_MAXIMUM = 60L * 1000L;
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
        DeliveryQueue[] queues = QueueOperations.select(null);
        // sort by queue name
        Arrays.sort(queues, new DeliveryQueueNameComparator());
        return queues;
    }

    /**
     * DeliveryQueue Comparator which compares based on queue name.
     */
    public static class DeliveryQueueNameComparator implements Comparator<DeliveryQueue> {
        /**
         * Compares two DeliveryQueue objects by name.
         * @param queue         The first object to be compared.
         * @param otherQueue    The second object to be compared.
         * @return              The result of the comparison.
         */
        @Override
        public int compare(DeliveryQueue queue, DeliveryQueue otherQueue) {
            return queue.getQueueName().compareTo(otherQueue.getQueueName());
        }
    }

    /**
     * Enables the delivery of the given Trading Networks delivery queue.
     *
     * @param queue             The queue to enable delivery on.
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
     * @param queue             The queue to enable delivery on.
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
     * @param queue             The queue to enable delivery on.
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
     * @param queue             The queue to enable delivery on.
     * @throws ServiceException If a database error occurs.
     */
    public static void suspend(DeliveryQueue queue) throws ServiceException {
        if (queue == null) return;
        queue.setState(DeliveryQueue.STATE_SUSPENDED);
        save(queue);
    }

    /**
     * Runs the given DeliveryQueue's scheduled task immediately.
     *
     * @param queue The DeliveryQueue whose scheduled task to run immediately.
     */
    public static void expedite(DeliveryQueue queue) throws ServiceException {
        ScheduleHelper.expedite(getScheduledTask(queue));
    }

    /**
     * Returns the scheduled task related to the given DeliveryQueue.
     *
     * @param queue             The queue whose scheduled task is to be returned.
     * @return                  The scheduled task related to the given queue.
     * @throws ServiceException If an error occurs.
     */
    private static String getScheduledTask(DeliveryQueue queue) throws ServiceException {
        String queueTaskID = null;
        if (queue != null) {
            IData[] tasks = ScheduleHelper.list(DELIVER_BATCH_SERVICE_NAME, null, null);
            for (IData task : tasks) {
                if (task != null) {
                    IDataCursor taskCursor = task.getCursor();
                    try {
                        String taskID = IDataCursorHelper.get(taskCursor, String.class, "id");
                        if (taskID != null) {
                            IData pipeline = IDataCursorHelper.get(taskCursor, IData.class, "pipeline");
                            if (pipeline != null) {
                                IDataCursor pipelineCursor = pipeline.getCursor();
                                try {
                                    String queueName = IDataCursorHelper.get(pipelineCursor, String.class, "queue");
                                    if (queue.getQueueName().equals(queueName)) {
                                        queueTaskID = taskID;
                                        break;
                                    }
                                } finally {
                                    pipelineCursor.destroy();
                                }
                            }
                        }
                    } finally {
                        taskCursor.destroy();
                    }
                }
            }
        }
        return queueTaskID;
    }

    /**
     * Returns the number of jobs currently queued in the given Trading Networks delivery queue.
     *
     * @param queue             The queue to return the length of.
     * @return                  The length of the given queue, which is the number of delivery jobs with a status
     *                          of QUEUED or DELIVERING.
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
     * Returns true if the given Trading Networks delivery queue is enabled or draining, and therefore should process
     * queued tasks.
     *
     * @param queue The queue to check whether it should process queued tasks.
     * @return      True if the given queue should process queued tasks.
     */
    public static boolean isProcessing(DeliveryQueue queue) {
        return queue != null && (queue.isEnabled() || queue.isDraining());
    }

    /**
     * Updates the given Trading Networks delivery queue with any changes that may have occurred.
     *
     * @param queue             The queue whose changes are to be saved.
     * @throws ServiceException If a database error occurs.
     */
    public static void save(DeliveryQueue queue) throws ServiceException {
        if (queue == null) return;

        try {
            IData pipeline = IDataFactory.create();
            IDataCursor cursor = pipeline.getCursor();
            try {
                IDataHelper.put(cursor, "queue", queue);
            } finally {
                cursor.destroy();
            }

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
     * @param age           The minimum age in milliseconds a job must be before it can be dequeued.
     * @param ignoreIDs     An optional set of task identities that can be ignored even if still queued.
     * @param maxSize       The maximum number of jobs to be returned in one call.
     * @return              The job at the head of the given queue, or null if the queue is empty.
     * @throws SQLException If a database error occurs.
     */
    public static List<GuaranteedJob> peek(DeliveryQueue queue, boolean ordered, Duration age, Set<String> ignoreIDs, int maxSize) throws SQLException {
        return peek(queue, ordered, age == null ? DEFAULT_DELIVERY_JOB_AGE_THRESHOLD_MILLISECONDS : age.getTimeInMillis(new Date()), ignoreIDs, maxSize);
    }

    /**
     * Returns the head of the given delivery queue without dequeuing it.
     *
     * @param queue         The delivery queue whose head job is to be returned.
     * @param ordered       Whether jobs should be dequeued in strict creation datetime first in first out (FIFO) order.
     * @param age           The minimum age in milliseconds a job must be before it can be dequeued.
     * @param ignoreIDs     An optional set of task identities that can be ignored even if still queued.
     * @param maxSize       The maximum number of jobs to be returned in one call.
     * @return              The job at the head of the given queue, or null if the queue is empty.
     * @throws SQLException If a database error occurs.
     */
    public static List<GuaranteedJob> peek(DeliveryQueue queue, boolean ordered, long age, Set<String> ignoreIDs, int maxSize) throws SQLException {
        if (queue == null) return null;
        if (age < 0L) age = 0L;
        if (ignoreIDs == null) ignoreIDs = Collections.emptySet();
        if (maxSize < 1) maxSize = 1;

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet results = null;
        List<GuaranteedJob> jobs = new ArrayList<GuaranteedJob>();

        try {
            connection = Datastore.getConnection();
            statement = connection.prepareStatement(ordered ? SELECT_NEXT_DELIVERY_JOB_ORDERED_SQL : SELECT_NEXT_DELIVERY_JOB_UNORDERED_SQL);
            statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
            statement.clearParameters();
            statement.setFetchSize(maxSize);

            int index = 0;
            String queueName = queue.getQueueName();
            SQLWrappers.setChoppedString(statement, ++index, queueName, "DeliveryQueue.QueueName");
            if (ordered) {
                SQLWrappers.setChoppedString(statement, ++index, queueName, "DeliveryQueue.QueueName");
            }

            Timestamp timestamp = new Timestamp(System.currentTimeMillis() - age);
            SQLWrappers.setTimestamp(statement, ++index, timestamp);
            SQLWrappers.setTimestamp(statement, ++index, timestamp);

            results = statement.executeQuery();

            while (results.next() && jobs.size() < maxSize) {
                String id = results.getString(1);
                if (!ignoreIDs.contains(id)) {
                    GuaranteedJob job = GuaranteedJobHelper.get(id);
                    if (job != null) {
                        jobs.add(job);
                    }
                }
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

        return jobs;
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
        return pop(queue, ordered, null);
    }

    /**
     * Dequeues the job at the head of the given delivery queue.
     *
     * @param queue         The delivery queue to dequeue the head job from.
     * @param ordered       Whether jobs should be dequeued in strict creation datetime first in first out (FIFO) order.
     * @param age           The minimum age a job must be before it can be dequeued.
     * @return              The dequeued job that was at the head of the given queue, or null if queue is empty.
     * @throws SQLException If a database error occurs.
     */
    public static GuaranteedJob pop(DeliveryQueue queue, boolean ordered, Duration age) throws SQLException {
        return pop(queue, ordered, age == null ? DEFAULT_DELIVERY_JOB_AGE_THRESHOLD_MILLISECONDS : age.getTimeInMillis(new Date()));
    }

    /**
     * Dequeues the job at the head of the given delivery queue.
     *
     * @param queue         The delivery queue to dequeue the head job from.
     * @param ordered       Whether jobs should be dequeued in strict creation datetime first in first out (FIFO) order.
     * @param age           The minimum age in milliseconds a job must be before it can be dequeued.
     * @return              The dequeued job that was at the head of the given queue, or null if queue is empty.
     * @throws SQLException If a database error occurs.
     */
    public static GuaranteedJob pop(DeliveryQueue queue, boolean ordered, long age) throws SQLException {
        List<GuaranteedJob> jobs;
        while(!(jobs = peek(queue, ordered, age, null, 1)).isEmpty()) {
            for (GuaranteedJob job : jobs) {
                GuaranteedJobHelper.setDelivering(job);
                // multiple threads or processes may be competing for queued tasks, so we will only return the job at the
                // head of the queue if this thread was able to set the job status to delivering
                if (job.isDelivering()) return job;
            }
        }
        return null;
    }

    /**
     * Returns the number of queued jobs in the given delivery queue.
     *
     * @param queue         The delivery queue.
     * @param ordered       Whether jobs should be dequeued in strict creation datetime first in first out (FIFO) order.
     * @param age           The minimum age in milliseconds a job must be before it can be dequeued.
     * @return              The number of queued jobs in the queue.
     * @throws SQLException If a database error occurs.
     */
    public static long size(DeliveryQueue queue, boolean ordered, Duration age) throws SQLException {
        return size(queue == null ? null : queue.getQueueName(), ordered, age);
    }

    /**
     * Returns the number of queued jobs in the given delivery queue.
     *
     * @param queueName     The delivery queue name.
     * @param ordered       Whether jobs should be dequeued in strict creation datetime first in first out (FIFO) order.
     * @param age           The minimum age in milliseconds a job must be before it can be dequeued.
     * @return              The number of queued jobs in the queue.
     * @throws SQLException If a database error occurs.
     */
    public static long size(String queueName, boolean ordered, Duration age) throws SQLException {
        return size(queueName, ordered, age == null ? DEFAULT_DELIVERY_JOB_AGE_THRESHOLD_MILLISECONDS : age.getTimeInMillis(new Date()));
    }

    /**
     * Returns the number of queued jobs in the given delivery queue.
     *
     * @param queueName     The delivery queue name.
     * @param ordered       Whether jobs should be dequeued in strict creation datetime first in first out (FIFO) order.
     * @param age           The minimum age in milliseconds a job must be before it can be dequeued.
     * @return              The number of queued jobs in the queue.
     * @throws SQLException If a database error occurs.
     */
    public static long size(String queueName, boolean ordered, long age) throws SQLException {
        long size = 0L;

        if (queueName != null) {
            if (age < 0L) age = 0L;

            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet results = null;

            try {
                connection = Datastore.getConnection();
                statement = connection.prepareStatement(ordered ? SELECT_COUNT_QUEUED_JOBS_ORDERED_SQL : SELECT_COUNT_QUEUED_JOBS_UNORDERED_SQL);
                statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
                statement.clearParameters();

                int index = 0;
                SQLWrappers.setChoppedString(statement, ++index, queueName, "DeliveryQueue.QueueName");
                if (ordered) {
                    SQLWrappers.setChoppedString(statement, ++index, queueName, "DeliveryQueue.QueueName");
                }

                Timestamp timestamp = new Timestamp(System.currentTimeMillis() - age);
                SQLWrappers.setTimestamp(statement, ++index, timestamp);
                SQLWrappers.setTimestamp(statement, ++index, timestamp);

                results = statement.executeQuery();

                if (results.next()) {
                    size = results.getLong(1);
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
        }

        return size;
    }

    /**
     * Returns the set of queue names that currently have pending QUEUED jobs.
     *
     * @return              The set of queue names that currently have pending QUEUED jobs.
     * @throws SQLException If a database error occurs.
     */
    public static Set<String> getPendingQueues() throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet results = null;
        Set<String> queues = Collections.emptySet();

        try {
            connection = Datastore.getConnection();
            statement = connection.prepareStatement(SELECT_QUEUES_WITH_QUEUED_TASKS_SQL);
            statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
            statement.clearParameters();

            int index = 0;
            Timestamp timestamp = new Timestamp(System.currentTimeMillis() - DEFAULT_DELIVERY_JOB_AGE_THRESHOLD_MILLISECONDS);
            SQLWrappers.setTimestamp(statement, ++index, timestamp);
            SQLWrappers.setTimestamp(statement, ++index, timestamp);

            results = statement.executeQuery();

            boolean setCreated = false;

            while(results.next()) {
                if (!setCreated) {
                    queues = new TreeSet<String>();
                    setCreated = true;
                }
                queues.add(results.getString(1));
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

        return queues;
    }

    /**
     * Returns the count of delivery queues that are currently enabled.
     *
     * @return              The count of delivery queues that are currently enabled.
     * @throws SQLException If a database error occurs.
     */
    public static long getEnabledQueueCount() throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet results = null;
        long count = 0;

        try {
            connection = Datastore.getConnection();
            statement = connection.prepareStatement(SELECT_COUNT_ENABLED_QUEUES_SQL);
            statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
            statement.clearParameters();

            results = statement.executeQuery();

            while(results.next()) {
                count = results.getLong(1);
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

        return count;
    }

    /**
     * Returns true if there are queues whose delivery service matches the given pattern.
     *
     * @param servicePattern    The pattern to be matched.
     * @param enabledOnly       Whether to only consider enabled queues.
     * @return                  True if there is at least one queue whose delivery service matches the given pattern.
     * @throws IOException      If an IO error occurs.
     * @throws SQLException     If a SQL error occurs.
     */
    public static boolean hasQueuesProcessedByService(Pattern servicePattern, boolean enabledOnly) throws IOException, SQLException {
        boolean hasQueuesProcessedByService = false;
        if (servicePattern != null) {
            DeliveryQueue[] queues = list();
            if (queues != null) {
                for (DeliveryQueue queue : queues) {
                    if (queue != null && (!enabledOnly || queue.isEnabled() || queue.isDraining())) {
                        DeliverySchedule schedule = queue.getSchedule();
                        if (schedule != null) {
                            String service = schedule.getService();
                            if (service != null && servicePattern.matcher(service).matches()) {
                                hasQueuesProcessedByService = true;
                                break;
                            }
                        }
                    }
                }
            }
        }
        return hasQueuesProcessedByService;
    }

    /**
     * Parser for the datetimes to be parsed in a DeliverySchedule object.
     */
    private static final String DELIVERY_SCHEDULE_DATETIME_PATTERN = "yyyy/MM/ddHH:mm:ss";

    /**
     * Returns the minimum delivery interval used by all the currently registered delivery queues.
     *
     * @param servicePattern    Optional regular expression for matching specific delivery services.
     * @return                  The minimum delivery interval used by all the currently registered delivery queues.
     * @throws IOException      If an IO error occurs.
     * @throws ParseException   If a datetime parsing error occurs.
     * @throws SQLException     If a SQL error occurs.
     */
    public static long getMinimumDeliveryInterval(Pattern servicePattern) throws IOException, ParseException, SQLException {
        long minimumDeliveryInterval = DEFAULT_DELIVERY_INTERVAL_MAXIMUM;

        DeliveryQueue[] queues = list();
        if (queues != null) {
            for (DeliveryQueue queue : queues) {
                if (queue != null) {
                    DeliverySchedule schedule = queue.getSchedule();
                    if (schedule != null && DeliverySchedule.TYPE_REPEATING.equals(schedule.getType())) {
                        String deliveryService = schedule.getService();
                        if (servicePattern == null || (deliveryService != null && servicePattern.matcher(deliveryService).matches())) {
                            long end = Long.MAX_VALUE;
                            String endDate = schedule.getEndDate(), endTime = schedule.getEndTime();
                            if (endDate != null && endTime != null) {
                                end = new SimpleDateFormat(DELIVERY_SCHEDULE_DATETIME_PATTERN).parse(endDate + endTime).getTime();
                            }

                            if (System.currentTimeMillis() < end) {
                                long interval = Long.parseLong(schedule.getInterval()) * 1000L;
                                if (interval < minimumDeliveryInterval) {
                                    minimumDeliveryInterval = interval;
                                }
                            }
                        }
                    }
                }
            }
        }

        return minimumDeliveryInterval;
    }

    /**
     * Returns the time in milliseconds of the next scheduled run of the given delivery queue.
     *
     * @param  queue            A delivery queue.
     * @return                  The time in milliseconds of the next scheduled run.
     * @throws ServiceException If a datetime parsing error occurs.
     */
    public static long nextRun(DeliveryQueue queue) throws ServiceException {
        DeliverySchedule schedule = queue.getSchedule();
        String type = schedule.getType();

        long next = 0L, start = 0L, end = 0L;

        try {
            String endDate = schedule.getEndDate(), endTime = schedule.getEndTime();
            if (endDate != null && endTime != null) {
                end = new SimpleDateFormat(DELIVERY_SCHEDULE_DATETIME_PATTERN).parse(endDate + endTime).getTime();
            }

            boolean noOverlap = BooleanHelper.parse(schedule.getNoOverlap());

            if (DeliverySchedule.TYPE_REPEATING.equals(type)) {
                next = getRepeatingNextRun(Long.parseLong(schedule.getInterval()) * 1000L, noOverlap, start, end);
            } else if (DeliverySchedule.TYPE_COMPLEX.equals(type)) {
                next = getComplexNextRun(Masks.buildLongMask(schedule.getMinutes()), Masks.buildIntMask(schedule.getHours()),
                        Masks.buildIntMask(schedule.getDaysOfMonth()), Masks.buildIntMask(schedule.getDaysOfWeek()),
                        Masks.buildIntMask(schedule.getMonths()), noOverlap, start, end);
            }
        } catch(ParseException ex) {
            ExceptionHelper.raise(ex);
        }

        return next;
    }

    /**
     * Use reflection to work around backwards incompatible changes to the scheduled task classes in 9.x and higher.
     */
    private static Constructor REPEATING_SCHEDULED_TASK_CONSTRUCTOR = null;
    private static Method REPEATING_SCHEDULED_TASK_IS_EXPIRED = null;
    private static Method REPEATING_SCHEDULED_TASK_CALC_NEXT_TIME = null;
    private static Method REPEATING_SCHEDULED_TASK_GET_NEXT_RUN = null;
    private static Constructor COMPLEX_SCHEDULED_TASK_CONSTRUCTOR = null;
    private static Method COMPLEX_SCHEDULED_TASK_IS_EXPIRED = null;
    private static Method COMPLEX_SCHEDULED_TASK_CALC_NEXT_TIME = null;
    private static Method COMPLEX_SCHEDULED_TASK_GET_NEXT_RUN = null;

    static {
        Class repeatingTaskClass = null, complexTaskClass = null;

        try {
            repeatingTaskClass = Class.forName("com.wm.app.b2b.server.scheduler.Simple");
        } catch(ClassNotFoundException ex) {
            try {
                repeatingTaskClass = Class.forName("com.wm.app.b2b.server.scheduler.ScheduledTask$Simple");
            } catch (ClassNotFoundException err) {
                // ignore exception
            }
        }

        if (repeatingTaskClass != null) {
            try {
                REPEATING_SCHEDULED_TASK_CONSTRUCTOR = repeatingTaskClass.getConstructor(long.class, boolean.class, long.class, long.class);
                REPEATING_SCHEDULED_TASK_IS_EXPIRED = repeatingTaskClass.getMethod("isExpired");
                REPEATING_SCHEDULED_TASK_CALC_NEXT_TIME = repeatingTaskClass.getMethod("calcNextTime");
                REPEATING_SCHEDULED_TASK_GET_NEXT_RUN = repeatingTaskClass.getMethod("getNextRun");
            } catch(NoSuchMethodException ex) {
                // ignore exception
            }
        }

        try {
            complexTaskClass = Class.forName("com.wm.app.b2b.server.scheduler.Mask");
        } catch(ClassNotFoundException ex) {
            try {
                complexTaskClass = Class.forName("com.wm.app.b2b.server.scheduler.ScheduledTask$Mask");
            } catch (ClassNotFoundException err) {
                // ignore exception
            }
        }

        if (complexTaskClass != null) {
            try {
                COMPLEX_SCHEDULED_TASK_CONSTRUCTOR = complexTaskClass.getConstructor(long.class, int.class, int.class, int.class, int.class, boolean.class, long.class, long.class);
                COMPLEX_SCHEDULED_TASK_IS_EXPIRED = complexTaskClass.getMethod("isExpired");
                COMPLEX_SCHEDULED_TASK_CALC_NEXT_TIME = complexTaskClass.getMethod("calcNextTime");
                COMPLEX_SCHEDULED_TASK_GET_NEXT_RUN = complexTaskClass.getMethod("getNextRun");
            } catch(NoSuchMethodException ex) {
                // ignore exception
            }
        }
    }

    /**
     * Returns the next time a simple repeating task with the given parameters should run.
     *
     * @param interval      The repeat interval in milliseconds.
     * @param runFromEnd    Whether tasks should not be overlapped.
     * @param start         The start time of the task.
     * @param end           The end time of the task.
     * @return              The next time the task should run.
     */
    private static long getRepeatingNextRun(long interval, boolean runFromEnd, long start, long end) {
        long nextRun = 0;

        try {
            Object task = REPEATING_SCHEDULED_TASK_CONSTRUCTOR.newInstance(interval, runFromEnd, start, end);

            boolean isExpired = (Boolean)REPEATING_SCHEDULED_TASK_IS_EXPIRED.invoke(task);
            if (!isExpired) {
                REPEATING_SCHEDULED_TASK_CALC_NEXT_TIME.invoke(task);
                nextRun = (Long) REPEATING_SCHEDULED_TASK_GET_NEXT_RUN.invoke(task);
            }
        } catch(IllegalAccessException ex) {
            throw new RuntimeException(ex);
        } catch(InstantiationException ex) {
            throw new RuntimeException(ex);
        } catch(InvocationTargetException ex) {
            throw new RuntimeException(ex);
        }

        return nextRun;
    }

    /**
     * Returns the next time a complex repeating task with the given parameters should run.
     *
     * @param minuteMask        The minute mask for the task.
     * @param hourMask          The hour mask for the task.
     * @param dayOfMonthMask    The day of month mask for the task.
     * @param dayOfWeekMask     The day of week mask for the task.
     * @param monthMask         The month mask for the task.
     * @param runFromEnd        Whether tasks should not be overlapped.
     * @param start             The start time of the task.
     * @param end               The end time of the task.
     * @return                  The next time the task should run.
     */
    private static long getComplexNextRun(long minuteMask, int hourMask, int dayOfMonthMask, int dayOfWeekMask, int monthMask, boolean runFromEnd, long start, long end) {
        long nextRun = 0;

        try {
            Object task = COMPLEX_SCHEDULED_TASK_CONSTRUCTOR.newInstance(minuteMask, hourMask, dayOfMonthMask, dayOfWeekMask, monthMask, runFromEnd, start, end);

            boolean isExpired = (Boolean)COMPLEX_SCHEDULED_TASK_IS_EXPIRED.invoke(task);
            if (!isExpired) {
                COMPLEX_SCHEDULED_TASK_CALC_NEXT_TIME.invoke(task);
                nextRun = (Long) COMPLEX_SCHEDULED_TASK_GET_NEXT_RUN.invoke(task);
            }
        } catch(IllegalAccessException ex) {
            throw new RuntimeException(ex);
        } catch(InstantiationException ex) {
            throw new RuntimeException(ex);
        } catch(InvocationTargetException ex) {
            throw new RuntimeException(ex);
        }

        return nextRun;
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
                        statusSilence = IDataHelper.getOrDefault(cursor, "$status.silence?", Boolean.class, false);
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
     * @param input             The queue to convert to an IData doc representation.
     * @return                  An IData doc representation of the given queue.
     * @throws ServiceException If a database error occurs.
     */
    public static IData toIData(DeliveryQueue input) throws ServiceException {
        if (input == null) return null;

        IData output = IDataFactory.create();
        IDataCursor cursor = output.getCursor();
        try {
            IDataHelper.put(cursor, "name", input.getQueueName());
            IDataHelper.put(cursor, "type", input.getQueueType());
            IDataHelper.put(cursor, "status", input.getState());
            IDataHelper.put(cursor, "length", "" + length(input));
            IDataHelper.put(cursor, "queue", input);
        } finally {
            cursor.destroy();
        }

        return output;
    }

    /**
     * Converts the given list of Trading Networks delivery queues to an IData[] doc list.
     *
     * @param input             The list of queues to convert to an IData[] doc list representation.
     * @return                  An IData[] doc list representation of the given queues.
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
