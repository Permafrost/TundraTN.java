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
import com.wm.app.tn.delivery.GuaranteedJob;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.data.IDataUtil;
import permafrost.tundra.lang.ExceptionHelper;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A collection of convenience methods for working with Trading Networks delivery queues.
 */
public class DeliveryQueueHelper {
    /**
     * SQL statement to select head of a delivery queue in job creation datetime order.
     */
    private static final String SELECT_NEXT_DELIVERY_JOB_ORDERED_SQL = "SELECT JobID FROM DeliveryJob WHERE QueueName = ? AND JobStatus = 'QUEUED' AND TimeCreated = (SELECT MIN(TimeCreated) FROM DeliveryJob WHERE QueueName = ? AND JobStatus = 'QUEUED') AND TimeUpdated <= ?";

    /**
     * SQL statement to select head of a delivery queue in indeterminate order.
     */
    private static final String SELECT_NEXT_DELIVERY_JOB_UNORDERED_SQL = "SELECT JobID FROM DeliveryJob WHERE QueueName = ? AND JobStatus = 'QUEUED' AND TimeCreated = (SELECT MIN(TimeCreated) FROM DeliveryJob WHERE QueueName = ? AND JobStatus = 'QUEUED' AND TimeUpdated <= ?)";

    /**
     * Disallow instantiation of this class.
     */
    private DeliveryQueueHelper() {}

    /**
     * Returns the Trading Networks delivery queue associated with the given name.
     *
     * @param queueName The name of the queue to return.
     * @return The delivery queue with the given name.
     * @throws ServiceException If a database error occurs.
     */
    public static DeliveryQueue get(String queueName) throws ServiceException {
        if (queueName == null) return null;

        DeliveryQueue queue = null;

        try {
            queue = QueueOperations.selectByName(queueName);
        } catch(SQLException ex) {
            ExceptionHelper.raise(ex);
        } catch(IOException ex) {
            ExceptionHelper.raise(ex);
        }

        return queue;
    }

    /**
     * Refreshes the given Trading Networks delivery queue from the database.
     *
     * @param queue The queue to be refreshed.
     * @return      The given queue, refreshed from the database.
     * @throws ServiceException If a database error occurs.
     */
    public static DeliveryQueue refresh(DeliveryQueue queue) throws ServiceException {
        return get(queue.getQueueName());
    }

    /**
     * Returns a list of all registered Trading Networks delivery queues.
     *
     * @return A list of all registered Trading Networks delivery queues.
     * @throws ServiceException If a database error occurs.
     */
    public static DeliveryQueue[] list() throws ServiceException {
        DeliveryQueue[] output = null;

        try {
            output = QueueOperations.select(null);
        } catch(SQLException ex) {
            ExceptionHelper.raise(ex);
        } catch(IOException ex) {
            ExceptionHelper.raise(ex);
        }

        return output;
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

        IData pipeline = IDataFactory.create();
        IDataCursor cursor = pipeline.getCursor();
        IDataUtil.put(cursor, "queue", queue);
        cursor.destroy();

        try {
            Service.doInvoke("wm.tn.queuing", "updateQueue", pipeline);
        } catch(Exception ex) {
            ExceptionHelper.raise(ex);
        }
    }

    /**
     * Returns the head of the given delivery queue without dequeuing it.
     *
     * @param queue   The delivery queue whose head job is to be returned.
     * @param ordered Whether jobs should be dequeued in strict creation datetime or first in first out (FIFO) order.
     * @return        The job at the head of the given queue, or null if the queue is empty.
     * @throws ServiceException
     */
    public static GuaranteedJob peek(DeliveryQueue queue, boolean ordered) throws ServiceException {
        if (queue == null) return null;

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet results = null;
        GuaranteedJob task = null;

        try {
            connection = Datastore.getConnection();
            statement = connection.prepareStatement(ordered ? SELECT_NEXT_DELIVERY_JOB_ORDERED_SQL : SELECT_NEXT_DELIVERY_JOB_UNORDERED_SQL);
            statement.clearParameters();

            String queueName = queue.getQueueName();
            SQLWrappers.setChoppedString(statement, 1, queueName, "DeliveryQueue.QueueName");
            SQLWrappers.setChoppedString(statement, 2, queueName, "DeliveryQueue.QueueName");
            SQLWrappers.setTimestamp(statement, 3, new java.sql.Timestamp(new java.util.Date().getTime()));

            results = statement.executeQuery();
            if (results.next()) {
                String id = results.getString(1);
                task = GuaranteedJobHelper.get(id);
            }
            connection.commit();
        } catch (SQLException ex) {
            connection = Datastore.handleSQLException(connection, ex);
            ExceptionHelper.raise(ex);
        } finally {
            SQLWrappers.close(results);
            SQLWrappers.close(statement);
            Datastore.releaseConnection(connection);
        }

        return task;
    }

    /**
     * Dequeues the job at the head of the given delivery queue.
     *
     * @param queue   The delivery queue to dequeue the head job from.
     * @param ordered Whether jobs should be dequeued in strict creation datetime or first in first out (FIFO) order.
     * @return        The dequeued job that was at the head of the given queue, or null if queue is empty.
     * @throws ServiceException If a database error is encountered.
     */
    public static GuaranteedJob pop(DeliveryQueue queue, boolean ordered) throws ServiceException {
        GuaranteedJob task = peek(queue, ordered);
        GuaranteedJobHelper.setDelivering(task);
        return task;
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
