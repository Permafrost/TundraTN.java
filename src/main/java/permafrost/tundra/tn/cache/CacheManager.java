/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2023 Lachlan Dowding
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

package permafrost.tundra.tn.cache;

import com.wm.app.b2b.server.InvokeState;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.lang.StartableManager;
import permafrost.tundra.server.ServerLogHelper;
import permafrost.tundra.server.ServerThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages one or more cache providers providing the ability to start and stop and restart all providers at once.
 */
public class CacheManager extends StartableManager<String, CacheProvider<?, ?>> {
    /**
     * How often to refresh caches from source: every 30 minutes.
     */
    private static final long DEFAULT_CACHE_REFRESH_SCHEDULE_MILLISECONDS = 30 * 60 * 1000L;
    /**
     * How often to reset lazy caches: every 6 hours.
     */
    protected static final long DEFAULT_CACHE_RESET_SCHEDULE_MILLISECONDS = 6 * 60 * 60 * 1000L;
    /**
     * How often to restart the cache manager: every day.
     */
    protected static final long DEFAULT_RESTART_SCHEDULE_MILLISECONDS = 24 * 60 * 60 * 1000L;
    /**
     * The timeout used when waiting for tasks to complete while shutting down the executor.
     */
    private static final long SCHEDULER_SHUTDOWN_TIMEOUT_MILLISECONDS = 60 * 1000L;

    /**
     * Initialization on demand holder idiom.
     */
    private static class Holder {
        /**
         * The singleton instance of the class.
         */
        private static final CacheManager INSTANCE = new CacheManager();
    }

    /**
     * Returns the singleton instance of this class.
     *
     * @return The singleton instance of this class.
     */
    public static CacheManager getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * The scheduler used to periodically check for queues being suspended so it can automatically stop the related
     * task processor.
     */
    private ScheduledExecutorService scheduler;

    /**
     * Disallow instantiation of this class.
     */
    private CacheManager() {
        super(false);
        register(IDTypeCache.TypeToDescription.getInstance());
        register(IDTypeCache.DescriptionToType.getInstance());
        register(PartnerIDCache.InternalToExternal.getInstance());
        register(PartnerIDCache.ExternalToInternal.getInstance());
        register(ProfileCache.getInstance());
    }

    /**
     * Registers the given cache provider with the manager.
     *
     * @param cacheProvider     The cache provider to register.
     * @return                  If the cache provider was registered.
     */
    public boolean register(CacheProvider<?, ?> cacheProvider) {
        return super.register(cacheProvider.getCacheName(), cacheProvider);
    }

    /**
     * Unregisters the given cache provider from the manager.
     *
     * @param cacheProvider     The cache provider to unregister.
     * @return                  True if the processor was unregistered.
     */
    public boolean unregister(CacheProvider<?, ?> cacheProvider) {
        return super.unregister(cacheProvider.getCacheName(), cacheProvider);
    }

    /**
     * Starts all objects managed by this manager.
     */
    @Override
    public synchronized void start() {
        startup();
        super.start();
    }

    /**
     * Starts up the scheduled supervisor thread.
     */
    private synchronized void startup() {
        scheduler = Executors.newScheduledThreadPool(1, new ServerThreadFactory("TundraTN/Cache Manager", InvokeState.getCurrentState()));

        // restart the scheduler regularly as a safety measure
        scheduler.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    shutdown();
                    startup();
                } catch (Throwable ex) {
                    ServerLogHelper.log(ex);
                }
            }
        }, DEFAULT_RESTART_SCHEDULE_MILLISECONDS, DEFAULT_RESTART_SCHEDULE_MILLISECONDS, TimeUnit.MILLISECONDS);

        // reset all lazy caches to be empty
        scheduler.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    reset();
                } catch (Throwable ex) {
                    ServerLogHelper.log(ex);
                }
            }
        }, DEFAULT_CACHE_RESET_SCHEDULE_MILLISECONDS, DEFAULT_CACHE_RESET_SCHEDULE_MILLISECONDS, TimeUnit.MILLISECONDS);

        // refresh all caches from source
        scheduler.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    refresh();
                } catch (Throwable ex) {
                    ServerLogHelper.log(ex);
                }
            }
        }, DEFAULT_CACHE_REFRESH_SCHEDULE_MILLISECONDS, DEFAULT_CACHE_REFRESH_SCHEDULE_MILLISECONDS, TimeUnit.MILLISECONDS);
    }

    /**
     * Stops all objects managed by this manager.
     */
    @Override
    public synchronized void stop() {
        shutdown();
        super.stop();
    }

    /**
     * Shuts down the scheduled supervisor thread.
     */
    private synchronized void shutdown() {
        try {
            scheduler.shutdown();
            scheduler.awaitTermination(SCHEDULER_SHUTDOWN_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS);
            scheduler.shutdownNow();
        } catch (InterruptedException ex) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Refreshes all registered cache providers from the provider's source resource.
     */
    public synchronized void refresh() {
        List<Throwable> exceptions = new ArrayList<Throwable>();

        for (CacheProvider<?, ?> provider : REGISTRY.values()) {
            try {
                provider.refresh();
            } catch(Throwable exception) {
                exceptions.add(exception);
            }
        }

        if (!exceptions.isEmpty()) {
            ExceptionHelper.raiseUnchecked(exceptions);
        }
    }

    /**
     * Resets cache providers by clearing any lazy caches of entries.
     */
    public synchronized void reset() {
        List<Throwable> exceptions = new ArrayList<Throwable>();

        for (CacheProvider<?, ?> provider : REGISTRY.values()) {
            try {
                if (provider.isLazy()) {
                    provider.clear();
                }
            } catch(Throwable exception) {
                exceptions.add(exception);
            }
        }

        if (!exceptions.isEmpty()) {
            ExceptionHelper.raiseUnchecked(exceptions);
        }
    }
}
