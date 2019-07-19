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
import com.wm.app.b2b.server.Server;
import com.wm.app.b2b.server.ServerAPI;
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.db.Datastore;
import com.wm.app.tn.db.SQLStatements;
import com.wm.app.tn.db.SQLWrappers;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.app.tn.route.RoutingRule;
import com.wm.data.IData;
import com.wm.data.IDataFactory;
import permafrost.tundra.lang.Startable;
import permafrost.tundra.server.SchedulerHelper;
import permafrost.tundra.server.SchedulerStatus;
import permafrost.tundra.server.ServerThreadFactory;
import permafrost.tundra.time.DateTimeHelper;
import permafrost.tundra.tn.document.BizDocEnvelopeHelper;
import permafrost.tundra.util.concurrent.BoundedPriorityBlockingQueue;
import permafrost.tundra.util.concurrent.PrioritizedThreadPoolExecutor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Used to defer processing of bizdocs to a dedicated thread pool.
 */
public class Deferrer implements Startable {
    /**
     * Bizdoc user status used to defer routing.
     */
    public static final String BIZDOC_USER_STATUS_DEFERRED = "DEFERRED";
    /**
     * Bizdoc user status used to begin routing after it was previously deferred.
     */
    public static final String BIZDOC_USER_STATUS_ROUTING = "ROUTING";
    /**
     * SQL statement for seeding deferred work queue.
     */
    protected final static String SELECT_BIZDOCS_FOR_USERSTATUS = "SELECT DocID FROM BizDoc WHERE UserStatus = ? ORDER BY DocTimestamp ASC";
    /**
     * The default timeout for database queries.
     */
    protected static final int DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS = 30;
    /**
     * How long to keep threads alive in the pool when idle.
     */
    protected static final long DEFAULT_THREAD_KEEP_ALIVE_MILLISECONDS = 60 * 60 * 1000L;
    /**
     * How often to seed deferred bizdocs from the database.
     */
    protected static final long DEFAULT_SEED_SCHEDULE_MILLISECONDS = 60 * 1000L;
    /**
     * How often to clean up completed deferred routes from the cache.
     */
    protected static final long DEFAULT_CLEAN_SCHEDULE_MILLISECONDS = 60 * 1000L;
    /**
     * How often to clean up completed deferred routes from the cache.
     */
    protected static final long DEFAULT_RESTART_SCHEDULE_MILLISECONDS = 60 * 60 * 1000L;
    /**
     * The default maximum capacity for the work queue.
     */
    protected static final int DEFAULT_THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;
    /**
     * Maximum time to wait to shutdown the executor in milliseconds.
     */
    protected static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLISECONDS = 5 * 60 * 1000L;
    /**
     * The maximum number of routes to cache in memory or to queue in the executor.
     */
    protected static final int DEFAULT_WORK_QUEUE_CAPACITY = DEFAULT_THREAD_POOL_SIZE * 2048;
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
     * An in-memory cache of pending routes.
     */
    protected ConcurrentMap<String, Future<IData>> pendingRoutes;

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
        setConcurrency(concurrency);
        setCapacity(capacity);
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
     * Defers the given bizdoc to be routed by a dedicated thread pool.
     *
     * @param bizdoc            The bizdoc to be deferred.
     * @throws ServiceException If an error occurs.
     */
    public Future<IData> defer(BizDocEnvelope bizdoc, RoutingRule rule, IData parameters) throws ServiceException {
        return defer(new CallableDeferredRoute(bizdoc, rule, parameters));
    }

    /**
     * Defers the given route to be executed by a dedicated thread pool.
     *
     * @param route             The route to be deferred.
     */
    public Future<IData> defer(CallableDeferredRoute route) {
        Future<IData> result = null;

        try {
            result = executor.submit(route);
            return result;
        } catch(Throwable ex) {
            result = new FutureTask<IData>(route);
        } finally {
            pendingRoutes.putIfAbsent(route.getIdentity(), result);
        }

        // if we failed to submit route to executor, then run it directly here
        try {
            result.get();
        } catch(InterruptedException ex) {
            // do nothing
        } catch(ExecutionException ex) {
            // do nothing
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
     * Returns the current capacity of the work queue.
     *
     * @return the current capacity of the work queue.
     */
    public int getCapacity() {
        return capacity;
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
     * Returns true if there are less threads busy than are in the pool.
     *
     * @return true if there are less threads busy than are in the pool.
     */
    public boolean isIdle() {
        return executor.getActiveCount() < executor.getMaximumPoolSize();
    }

    /**
     * Returns true if the work queue is full.
     *
     * @return true if the work queue is full.
     */
    public boolean isSaturated() {
        return executor.getQueue().size() >= capacity;
    }

    /**
     * Returns the number of routes pending execution.
     *
     * @return the number of routes pending execution.
     */
    public int size() {
        return executor.getQueue().size();
    }

    /**
     * Seeds the work queue with any bizdocs in the database with user status "DEFERRED".
     */
    protected void seed() {
        if (shouldSeed()) {
            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;

            try {
                connection = Datastore.getConnection();

                statement = connection.prepareStatement(SELECT_BIZDOCS_FOR_USERSTATUS);
                statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
                statement.clearParameters();
                SQLWrappers.setString(statement, 1, BIZDOC_USER_STATUS_DEFERRED);

                boolean shouldContinue = true;

                while (shouldSeed() && shouldContinue) {
                    shouldContinue = false;

                    try {
                        resultSet = statement.executeQuery();
                        while (shouldSeed() && resultSet.next()) {
                            String id = resultSet.getString(1);
                            try {
                                if (!pendingRoutes.containsKey(id)) {
                                    pendingRoutes.putIfAbsent(id, executor.submit(new CallableDeferredRoute(id)));
                                    shouldContinue = true;
                                }
                            } catch (RejectedExecutionException ex) {
                                // executor must be saturated or shut down, so stop seeding
                                shouldContinue = false;
                                break;
                            }
                        }
                    } finally {
                        SQLWrappers.close(resultSet);
                        connection.commit();
                    }
                }
            } catch (SQLException ex) {
                connection = Datastore.handleSQLException(connection, ex);
                ServerAPI.logError(ex);
            } finally {
                SQLStatements.releaseStatement(statement);
                Datastore.releaseConnection(connection);
            }
        }
    }

    /**
     * Returns true if seeding can occur on this server.
     *
     * @return true if seeding can occur on this server.
     */
    protected boolean shouldSeed() {
        return isStarted() && Server.isRunning() && SchedulerHelper.status() == SchedulerStatus.STARTED && !isSaturated();
    }

    /**
     * Cleans up the deferred route cache by removing bizdocs that are no longer deferred.
     */
    protected void clean() {
        for (Map.Entry<String, Future<IData>> pendingRoute : pendingRoutes.entrySet()) {
            String id = pendingRoute.getKey();
            Future<IData> result = pendingRoute.getValue();
            if (result.isDone()) {
                pendingRoutes.remove(id, result);
            }
        }
    }

    /**
     * Starts this object.
     */
    @Override
    public synchronized void start() {
        start(null);
    }

    /**
     * Starts this object.
     *
     * @param pendingTasks  A list of tasks that were previously pending execution that will be resubmitted.
     */
    protected synchronized void start(List<Runnable> pendingTasks) {
        if (!started) {
            ConcurrentMap<String, Future<IData>> drainedRoutes = pendingRoutes;
            pendingRoutes = new ConcurrentHashMap<String, Future<IData>>(capacity);
            if (drainedRoutes != null) {
                pendingRoutes.putAll(drainedRoutes);
            }

            ThreadFactory threadFactory = new ServerThreadFactory("TundraTN/Defer Worker", null, InvokeState.getCurrentState(), Thread.NORM_PRIORITY, false);
            executor = new PrioritizedThreadPoolExecutor(concurrency, concurrency, DEFAULT_THREAD_KEEP_ALIVE_MILLISECONDS, TimeUnit.MILLISECONDS, new BoundedPriorityBlockingQueue<Runnable>(capacity), threadFactory, new ThreadPoolExecutor.AbortPolicy());
            executor.allowCoreThreadTimeOut(true);

            scheduler = Executors.newScheduledThreadPool(1, new ServerThreadFactory("TundraTN/Defer Seeder", InvokeState.getCurrentState()));

            // schedule seeding
            scheduler.scheduleWithFixedDelay(new Runnable() {
                public void run() {
                    try {
                        seed();
                    } catch(Throwable ex) {
                        ServerAPI.logError(ex);
                    }
                }
            }, DEFAULT_SEED_SCHEDULE_MILLISECONDS, DEFAULT_SEED_SCHEDULE_MILLISECONDS, TimeUnit.MILLISECONDS);

            // schedule cleaning
            scheduler.scheduleWithFixedDelay(new Runnable() {
                public void run() {
                    try {
                        clean();
                    } catch(Throwable ex) {
                        ServerAPI.logError(ex);
                    }
                }
            }, DEFAULT_CLEAN_SCHEDULE_MILLISECONDS, DEFAULT_CLEAN_SCHEDULE_MILLISECONDS, TimeUnit.MILLISECONDS);

            // schedule restart
            scheduler.scheduleWithFixedDelay(new Runnable() {
                public void run() {
                    try {
                        restart();
                    } catch(Throwable ex) {
                        ServerAPI.logError(ex);
                    }
                }
            }, DEFAULT_RESTART_SCHEDULE_MILLISECONDS, DEFAULT_RESTART_SCHEDULE_MILLISECONDS, TimeUnit.MILLISECONDS);

            if (pendingTasks != null) {
                for (Runnable pendingTask : pendingTasks) {
                    try {
                        executor.submit(pendingTask);
                    } catch(RejectedExecutionException ex) {
                        // do nothing
                    }
                }
            }

            clean();

            started = true;
        }
    }

    /**
     * Stops this object.
     */
    @Override
    public synchronized void stop() {
        stop(DEFAULT_SHUTDOWN_TIMEOUT_MILLISECONDS, true);
    }

    /**
     * Stops this object.
     *
     * @param timeout   How long in milliseconds to wait for the executor to shutdown.
     * @param interrupt Whether to interrupt tasks that are in-flight.
     * @return          List of submitted tasks not yet started.
     */
    protected synchronized List<Runnable> stop(long timeout, boolean interrupt) {
        List<Runnable> pendingTasks = Collections.emptyList();

        if (started) {
            started = false;

            try {
                scheduler.shutdown();
                executor.shutdown();
                if (timeout > 0) {
                    executor.awaitTermination(timeout, TimeUnit.MILLISECONDS);
                }
            } catch(InterruptedException ex) {
                // ignore interruption to this thread
            } finally {
                if (interrupt) {
                    scheduler.shutdownNow();
                    pendingTasks = executor.shutdownNow();
                } else {
                    pendingTasks = drain();
                }
            }
        }

        return pendingTasks;
    }

    /**
     * Drains the executor work queue of tasks not yet started.
     *
     * @return List of submitted tasks not yet started.
     */
    protected List<Runnable> drain() {
        BlockingQueue<Runnable> queue = executor.getQueue();
        ArrayList<Runnable> list = new ArrayList<Runnable>(queue.size());
        queue.drainTo(list);
        if (!queue.isEmpty()) {
            for (Runnable runnable : queue.toArray(new Runnable[0])) {
                if (queue.remove(runnable)) {
                    list.add(runnable);
                }
            }
        }
        return list;
    }

    /**
     * Restarts this object.
     */
    @Override
    public synchronized void restart() {
        start(stop(0, false));
    }

    /**
     * Returns true if the object is started.
     *
     * @return true if the object is started.
     */
    @Override
    public boolean isStarted() {
        return started;
    }

    /**
     * A callable deferred route with optimistic concurrency via user status.
     */
    protected static class CallableDeferredRoute extends CallableRoute {
        /**
         * An in-memory cache of routes currently executing.
         */
        protected static final ConcurrentMap<String, CallableDeferredRoute> EXECUTING_ROUTES = new ConcurrentHashMap<String, CallableDeferredRoute>();

        /**
         * Constructs a new CallableDeferredRoute object.
         *
         * @param id                The internal ID of the bizdoc to be routed.
         */
        public CallableDeferredRoute(String id) {
            super(id);
        }

        /**
         * Constructs a new CallableRoute.
         *
         * @param bizdoc            The bizdoc to be routed.
         * @param rule              The rule to use when routing.
         * @param parameters        The optional TN_parms to use when routing.
         * @throws ServiceException If an error occurs.
         */
        public CallableDeferredRoute(BizDocEnvelope bizdoc, RoutingRule rule, IData parameters) throws ServiceException {
            super(bizdoc, rule, parameters);
            if (!BIZDOC_USER_STATUS_DEFERRED.equals(bizdoc.getUserStatus())) {
                BizDocEnvelopeHelper.setStatus(bizdoc, null, BIZDOC_USER_STATUS_DEFERRED);
            }
        }

        /**
         * Routes the bizdoc.
         */
        @Override
        public IData call() throws ServiceException {
            IData output;
            Thread currentThread = Thread.currentThread();

            if (EXECUTING_ROUTES.putIfAbsent(id, this) == null) {
                String currentThreadName = currentThread.getName();
                try {
                    initialize();

                    if (BizDocEnvelopeHelper.setUserStatusForPrevious(bizdoc, BIZDOC_USER_STATUS_ROUTING, BIZDOC_USER_STATUS_DEFERRED)) {
                        // status was able to be changed, so we have a "lock" on the bizdoc and can now route it
                        currentThread.setName(MessageFormat.format("{0}: BizDoc {1} PROCESSING {2}", currentThreadName, BizDocEnvelopeHelper.toLogString(bizdoc), DateTimeHelper.now("datetime")));
                        output = super.call();
                    } else {
                        // status was not able to be changed, therefore another server or process has already processed this bizdoc
                        output = IDataFactory.create();
                    }
                } finally {
                    EXECUTING_ROUTES.remove(id, this);
                    currentThread.setName(currentThreadName);
                }
            } else {
                output = IDataFactory.create();
            }

            return output;
        }
    }
}
