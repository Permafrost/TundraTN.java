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

package permafrost.tundra.tn.queue;

import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.db.QueueOperations;
import com.wm.app.tn.delivery.DeliveryQueue;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.data.IDataUtil;
import permafrost.tundra.lang.ExceptionHelper;

import java.io.IOException;
import java.sql.SQLException;

/**
 * A collection of convenience methods for working with Trading Networks delivery queues.
 */
public class QueueHelper {
    /**
     * Disallow instantiation of this class.
     */
    private QueueHelper() {}

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
     * Converts the given Trading Networks delivery queue to an IData document.
     *
     * @param input The queue to convert to an IData document representation.
     * @return      An IData document representation of the given queue.
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
     * Converts the given list of Trading Networks delivery queues to an IData[] document list.
     *
     * @param input The list of queues to convert to an IData[] document list representation.
     * @return      An IData[] document list representation of the given queues.
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
