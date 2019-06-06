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
import com.wm.app.tn.db.Datastore;
import com.wm.app.tn.db.SQLStatements;
import com.wm.app.tn.db.SQLWrappers;
import permafrost.tundra.lang.Startable;
import permafrost.tundra.lang.ThreadHelper;
import permafrost.tundra.server.ServerThreadFactory;
import permafrost.tundra.util.concurrent.PrioritizedThreadPoolExecutor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Used to defer processing of bizdocs to a dedicated thread pool.
 */
public class Deferrer implements Startable {
    /**
     * SQL statement for seeding deferred queue on startup.
     */
    protected final static String SELECT_DEFERRED_BIZDOCS_FOR_SEEDING = "SELECT DocID FROM BizDoc WHERE RoutingStatus = 'NOT ROUTED' AND UserStatus = 'DEFERRED' ORDER BY DocTimestamp ASC";
    /**
     * The default timeout for database queries.
     */
    private static final int DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS = 30;
    /**
     * How long to keep threads alive in the pool when idle.
     */
    private static final int DEFAULT_THREAD_KEEP_ALIVE_MILLISECONDS = 60 * 1000;
    /**
     * Is this object started or stopped?
     */
    protected volatile boolean started;
    /**
     * The executors used to run deferred jobs by thread priority.
     */
    protected ConcurrentMap<Integer, ThreadPoolExecutor> executors = new ConcurrentHashMap<Integer, ThreadPoolExecutor>();
    /**
     * The level of concurrency to use, equal to the number of threads in the pool used for processing.
     */
    protected int concurrency;
    /**
     * Maximum time to wait to shutdown the executor in nanoseconds.
     */
    protected long shutdownTimeout = 5L * 60 * 1000 * 1000 * 1000;

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
        this(Runtime.getRuntime().availableProcessors() * 2);
    }

    /**
     * Creates a new Deferrer.
     *
     * @param concurrency   The level of concurrency to use.
     */
    public Deferrer(int concurrency) {
        this.concurrency = concurrency;
        for(int i = Thread.MIN_PRIORITY; i <= Thread.MAX_PRIORITY; i++) {
            executors.put(i, createExecutor(i));
        }
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
     * Creates a new executor service.
     *
     * @param threadPriority    The thread priority the new executor will use.
     * @return                  The newly created executor.
     */
    protected ThreadPoolExecutor createExecutor(int threadPriority) {
        threadPriority = ThreadHelper.normalizePriority(threadPriority);
        ThreadPoolExecutor executor = new PrioritizedThreadPoolExecutor(concurrency, concurrency, DEFAULT_THREAD_KEEP_ALIVE_MILLISECONDS, TimeUnit.MILLISECONDS, new PriorityBlockingQueue<Runnable>(), new ServerThreadFactory(String.format("TundraTN/Defer Worker Priority=%02d", threadPriority), threadPriority, InvokeState.getCurrentState()), new ThreadPoolExecutor.AbortPolicy());
        executor.allowCoreThreadTimeOut(true);

        return executor;
    }

    /**
     * Defers the given route to be run by a dedicated thread pool.
     *
     * @param route  The route to be deferred.
     */
    public void defer(CallableRoute route) throws Exception {
        if (!isStarted()) throw new IllegalStateException("Deferrer must be started before it can accept deferred routes");

        if (route != null) {
            int threadPriority = route.getThreadPriority();

            ThreadPoolExecutor executor;

            if (executors.containsKey(threadPriority)) {
                executor = executors.get(threadPriority);
            } else {
                ThreadPoolExecutor newExecutor = createExecutor(threadPriority);
                executor = executors.putIfAbsent(threadPriority, newExecutor);
                if (executor == null) executor = newExecutor;
            }

            try {
                executor.submit(route);
            } catch(RejectedExecutionException ex) {
                // route on calling thread, as we must currently be shutting down the executors
                route.call();
            } catch(NullPointerException ex) {
                // route on calling thread, as deferrer must be shutting down
                route.call();
            }
        }
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
     * Allows the concurrency to be changed while running.
     *
     * @param concurrency   The level of concurrency to use.
     */
    public void setConcurrency(int concurrency) {
        if (concurrency < 1) throw new IllegalArgumentException("concurrency must be >= 1");

        this.concurrency = concurrency;
        if (isStarted()) {
            for (ThreadPoolExecutor executor : executors.values()) {
                executor.setCorePoolSize(concurrency);
                executor.setMaximumPoolSize(concurrency);
            }
        }
    }

    /**
     * Returns the number of queued tasks.
     *
     * @return the number of queued tasks.
     */
    public int size() {
        int size = 0;

        if (isStarted()) {
            for (ThreadPoolExecutor executor : executors.values()) {
                size += executor.getQueue().size();
            }
        }

        return size;
    }

    /**
     * Seeds the work queue with any bizdocs in the database with user status "DEFERRED".
     */
    public void seed() {
        if (isStarted()) {
            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;

            try {
                connection = Datastore.getConnection();
                statement = connection.prepareStatement(SELECT_DEFERRED_BIZDOCS_FOR_SEEDING);
                statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
                statement.clearParameters();

                resultSet = statement.executeQuery();
                while (resultSet.next()) {
                    try {
                        defer(new CallableRoute(resultSet.getString(1)));
                    } catch(Exception ex) {
                        // do nothing
                    }
                }

                connection.commit();
            } catch (SQLException ex) {
                connection = Datastore.handleSQLException(connection, ex);
                throw new RuntimeException(ex);
            } finally {
                SQLWrappers.close(resultSet);
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
            started = true;
        }
        seed();
    }

    /**
     * Stops this object.
     */
    @Override
    public synchronized void stop() {
        if (started) {
            started = false;
            try {
                for (ThreadPoolExecutor executor : executors.values()) {
                    executor.shutdown();
                }

                long endTime = System.nanoTime() + shutdownTimeout;
                for (ThreadPoolExecutor executor : executors.values()) {
                    long startTime = System.nanoTime();
                    if (startTime < endTime) {
                        executor.awaitTermination(endTime - startTime, TimeUnit.NANOSECONDS);
                    }
                }
            } catch(InterruptedException ex) {
                // ignore interruption to this thread
            } finally {
                for (ThreadPoolExecutor executor : executors.values()) {
                    executor.shutdownNow();
                }
                executors.clear();
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