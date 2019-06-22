/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Lachlan Dowding
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

package permafrost.tundra.tn.route;

import com.wm.app.b2b.server.InvokeState;
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.db.Datastore;
import com.wm.app.tn.db.SQLStatements;
import com.wm.app.tn.db.SQLWrappers;
import com.wm.data.IData;
import permafrost.tundra.lang.Startable;
import permafrost.tundra.server.SchedulerHelper;
import permafrost.tundra.server.SchedulerStatus;
import permafrost.tundra.server.ServerThreadFactory;
import permafrost.tundra.util.concurrent.BoundedPriorityBlockingQueue;
import permafrost.tundra.util.concurrent.ImmediateFuture;
import permafrost.tundra.util.concurrent.PrioritizedThreadPoolExecutor;
import javax.xml.datatype.Duration;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Used to defer processing of bizdocs to a dedicated thread pool.
 */
public class Deferrer implements Startable {
    /**
     * SQL statement for seeding deferred queue on startup.
     */
    protected final static String SELECT_DEFERRED_BIZDOCS_FOR_SEEDING = "SELECT DocID FROM BizDoc WHERE UserStatus = 'DEFERRED' AND LastModified <= ? AND DocTimestamp = (SELECT MIN(DocTimestamp) FROM BizDoc WHERE UserStatus = 'DEFERRED' AND LastModified <= ?)";
    /**
     * The default timeout for database queries.
     */
    protected static final int DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS = 30;
    /**
     * How long to keep threads alive in the pool when idle.
     */
    protected static final long DEFAULT_THREAD_KEEP_ALIVE_MILLISECONDS = 5 * 60 * 1000L;
    /**
     * How often to reseed from the database to self-heal after outages and load balance.
     */
    protected static final long DEFAULT_RESEED_SCHEDULE_MILLISECONDS = 60 * 5 * 1000L;
    /**
     * How old deferred documents need to be before they get reseeded.
     */
    protected static final long DEFAULT_RESEED_BIZDOC_AGE = 60 * 5 * 1000L;
    /**
     * The default maximum capacity for the work queue.
     */
    protected static final int DEFAULT_THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;
    /**
     * The default maximum capacity for the work queue.
     */
    protected static final int DEFAULT_WORK_QUEUE_CAPACITY = DEFAULT_THREAD_POOL_SIZE * 256;
    /**
     * Maximum time to wait to shutdown the executor in nanoseconds.
     */
    protected static final long DEFAULT_SHUTDOWN_TIMEOUT = 5L * 60 * 1000 * 1000 * 1000;
    /**
     * Is this object started or stopped?
     */
    protected volatile boolean started;
    /**
     * The executors used to run deferred jobs by thread priority.
     */
    protected volatile ThreadPoolExecutor executor;
    /**
     * The scheduler used to self-heal and load balance by seeding deferred documents from database regularly.
     */
    protected volatile ScheduledExecutorService scheduler;
    /**
     * The level of concurrency to use, equal to the number of threads in the pool used for processing.
     */
    protected volatile int concurrency;
    /**
     * The maximum capacity of the work queue.
     */
    protected volatile int capacity;

    /**
     * Initialization on demand holder idiom.
     */
    private static class Holder {
        /**
         * The singleton instance of the class.
         */
        private static final Deferrer DEFERRER = new Deferrer();
    }

    /**
     * Creates a new Deferrer.
     */
    public Deferrer() {
        this(DEFAULT_THREAD_POOL_SIZE);
    }

    /**
     * Creates a new Deferrer.
     *
     * @param concurrency   The level of concurrency to use.
     */
    public Deferrer(int concurrency) {
        this(concurrency, DEFAULT_WORK_QUEUE_CAPACITY);
    }

    /**
     * Creates a new Deferrer.
     *
     * @param concurrency   The level of concurrency to use.
     * @param capacity      The maximum capacity of the work queue.
     */
    public Deferrer(int concurrency, int capacity) {
        this.concurrency = concurrency;
        this.capacity = capacity;
    }

    /**
     * Returns the singleton instance of this class.
     *
     * @return the singleton instance of this class.
     */
    public static Deferrer getInstance() {
        return Holder.DEFERRER;
    }

    /**
     * Defers the given route to be run by a dedicated thread pool. Runs route immediately if deferrer has been
     * shutdown.
     *
     * @param route             The route to be deferred.
     * @return                  A future containing the result of the route.
     * @throws ServiceException If route throws an exception.
     */
    public Future<IData> defer(CallableRoute route) throws ServiceException {
        if (route == null) throw new NullPointerException("route must not be null");

        Future<IData> result = null;

        if (isStarted()) {
            result = executor.submit(route);
        }

        if (result == null) {
            // fallback to routing on the current thread, if deferrer is shutdown or otherwise busy
            result = new ImmediateFuture<IData>(route.call());
        }

        return result;
    }

    /**
     * Returns the current level of concurrency used.
     *
     * @return the current level of concurrency used.
     */
    public int getConcurrency() {
        return concurrency;
    }

    /**
     * Sets the size of the thread pool.
     *
     * @param concurrency   The level of concurrency to use.
     */
    public synchronized void setConcurrency(int concurrency) {
        if (concurrency < 1) throw new IllegalArgumentException("concurrency must be >= 1");
        this.concurrency = concurrency;
    }

    /**
     * Sets the maximum capacity of the work queue.
     *
     * @param capacity  The maximum capacity of the work queue.
     */
    public synchronized void setCapacity(int capacity) {
        if (capacity < 1) throw new IllegalArgumentException("capacity must be >= 1");
        this.capacity = capacity;
    }

    /**
     * Returns the number of queued tasks.
     *
     * @return the number of queued tasks.
     */
    public int size() {
        int size = 0;
        if (isStarted()) {
            size = executor.getQueue().size();
        }
        return size;
    }

    /**
     * Seeds the work queue with any bizdocs in the database with user status "DEFERRED", regardless of age.
     */
    public void seed() {
        seed(0);
    }

    /**
     * Seeds the work queue with any bizdocs in the database with user status "DEFERRED" with the given age.
     *
     * @param age   The age that candidate bizdocs must be before being seeded.
     */
    public void seed(Duration age) {
        seed(age.getTimeInMillis(Calendar.getInstance()));
    }

    /**
     * Seeds the work queue with any bizdocs in the database with user status "DEFERRED" with the given age.
     *
     * @param age   The age in milliseconds that candidate bizdocs must be before being seeded.
     */
    public void seed(long age) {
        if (isStarted()) {
            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;

            try {
                connection = Datastore.getConnection();
                statement = connection.prepareStatement(SELECT_DEFERRED_BIZDOCS_FOR_SEEDING);
                statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
                statement.clearParameters();

                Timestamp timestamp = new Timestamp(System.currentTimeMillis() - age);
                SQLWrappers.setTimestamp(statement, 1, timestamp);
                SQLWrappers.setTimestamp(statement, 2, timestamp);

                while (isStarted()) {
                    try {
                        resultSet = statement.executeQuery();
                        if (resultSet.next()) {
                            String id = resultSet.getString(1);
                            if (id == null) {
                                break;
                            } else {
                                try {
                                    Future<IData> result = defer(new CallableRoute(id));
                                    // wait for task to finish before seeding another, so as not to overwhelm the server by
                                    // applying back pressure to seeding
                                    result.get();
                                } catch (Exception ex) {
                                    // do nothing
                                }
                            }
                        } else {
                            break;
                        }
                    } finally {
                        SQLWrappers.close(resultSet);
                        connection.commit();
                    }
                }
            } catch (SQLException ex) {
                connection = Datastore.handleSQLException(connection, ex);
                throw new RuntimeException(ex);
            } finally {
                SQLStatements.releaseStatement(statement);
                Datastore.releaseConnection(connection);
            }
        }
    }

    /**
     * Starts this object.
     */
    @Override
    public synchronized void start() {
        if (!started) {
            ThreadFactory threadFactory = new ServerThreadFactory("TundraTN/Defer Worker", null, InvokeState.getCurrentState(), Thread.NORM_PRIORITY, false);
            executor = new PrioritizedThreadPoolExecutor(concurrency, concurrency, DEFAULT_THREAD_KEEP_ALIVE_MILLISECONDS, TimeUnit.MILLISECONDS, new BoundedPriorityBlockingQueue<Runnable>(capacity), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
            scheduler = Executors.newScheduledThreadPool(1, new ServerThreadFactory("TundraTN/Defer Seeder", InvokeState.getCurrentState()));
            scheduler.scheduleWithFixedDelay(new Runnable() {
                public void run() {
                    if (SchedulerHelper.status() == SchedulerStatus.STARTED) {
                        seed(DEFAULT_RESEED_BIZDOC_AGE);
                    }
                }
            }, DEFAULT_RESEED_SCHEDULE_MILLISECONDS, DEFAULT_RESEED_SCHEDULE_MILLISECONDS, TimeUnit.MILLISECONDS);

            started = true;

            seed();
        }
    }

    /**
     * Stops this object.
     */
    @Override
    public synchronized void stop() {
        if (started) {
            started = false;

            try {
                scheduler.shutdown();
                executor.shutdown();
                executor.awaitTermination(DEFAULT_SHUTDOWN_TIMEOUT, TimeUnit.NANOSECONDS);
            } catch(InterruptedException ex) {
                // ignore interruption to this thread
            } finally {
                scheduler.shutdownNow();
                executor.shutdownNow();
            }
        }
    }

    /**
     * Returns true if the object is started.
     * @return true if the object is started.
     */
    @Override
    public boolean isStarted() {
        return started;
    }
}
