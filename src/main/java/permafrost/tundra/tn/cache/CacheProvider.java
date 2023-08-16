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

import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.data.IDataPortable;
import com.wm.util.coder.IDataCodable;
import com.wm.util.coder.ValuesCodable;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.lang.Startable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Base class for cache providers.
 *
 * @param <K>   The class of cache keys.
 * @param <V>   The class of cache values.
 */
public abstract class CacheProvider<K,V> implements Startable, IDataCodable {
    /**
     * The default timeout for database queries.
     */
    protected static final int DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS = 30;
    /**
     * The cache provided by this object.
     */
    protected volatile ConcurrentMap<K, V> cache = new ConcurrentHashMap<K, V>();
    /**
     * Whether the cache is populated with entries lazily (that is, on first access) or not (that is, exhaustively
     * seeded with all entries on startup).
     */
    protected volatile boolean lazy = false;
    /**
     * Whether this cache provider is started.
     */
    protected volatile boolean started = false;

    /**
     * Returns the unique name of this cache provider.
     *
     * @return The unique name of this cache provider.
     */
    public abstract String getCacheName();

    /**
     * Seeds the cache will all entries from the cache's source resource such as a database.
     */
    public void seed() {
        seed(true);
    }

    /**
     * Seeds the cache will all entries from the cache's source resource such as a database.
     *
     * @param refresh   If true, will refresh already cached entries. If false, will only add missing entries.
     */
    public void seed(boolean refresh) {
        Map<K, V> source = fetch();
        if (source != null) {
            if (refresh) {
                cache.keySet().retainAll(source.keySet());
            } else {
                source.keySet().removeAll(cache.keySet());
            }
            cache.putAll(source);
        }
    }

    /**
     * Refreshes all current cache entries from the cache's source resource such as a database.
     */
    public void refresh() {
        Map<K, V> source = fetch();
        if (source != null) {
            source.keySet().retainAll(cache.keySet());
            cache.keySet().retainAll(source.keySet());
            cache.putAll(source);
        }
    }

    /**
     * Clears all entries from the cache.
     */
    public void clear() {
        cache.clear();
    }

    /**
     * Returns the cached value associated with the given key.
     *
     * @param key   The key whose value is to be returned.
     * @return      The value associated with the given key.
     */
    public V get(K key) {
        return get(key, false);
    }

    /**
     * Returns the cached value associated with the given key.
     *
     * @param key       The key whose value is to be returned.
     * @param refresh   Whether to refresh the cached value from the cache's source resource.
     * @return          The value associated with the given key.
     */
    public V get(K key, boolean refresh) {
        V value = null;

        if (!refresh) value = cache.get(key);

        if (refresh || value == null) {
            value = fetch(key);
            if (value == null) {
                // remove cached entries that no longer exist
                cache.remove(key);
            } else {
                // add missing or refreshed entry to cache
                cache.put(key, value);
            }
        }

        return value;
    }

    /**
     * Returns true if the given key exists in the cache.
     *
     * @param key   The key to check existence of.
     * @return      True if the given key exists in the cache.
     */
    public boolean exists(K key) {
        boolean exists;
        if (lazy) {
            exists = get(key) != null;
        } else {
            exists = cache.containsKey(key);
        }
        return exists;
    }

    /**
     * Fetches the value associated with the given key from the cache's source resource.
     *
     * @param key           The key whose value is to be fetched.
     * @return              The value associated with the given key fetched from source.
     */
    protected V fetch(K key) {
        V value = null;

        Map<K, V> source = fetch();
        if (source != null) {
            value = get(key);
        }

        return value;
    }

    /**
     * Fetches all keys and values from the cache's source resource.
     *
     * @return              All keys and values from the cache's source resource.
     */
    abstract protected Map<K, V> fetch();

    /**
     * Starts the cache provider.
     */
    @Override
    public void start() {
        if (!started) {
            started = true;
            if (!lazy) {
                seed();
            }
        }
    }

    /**
     * Stops the cache provider.
     */
    @Override
    public void stop() {
        if (started) {
            started = false;
            if (lazy) {
                clear();
            }
        }
    }

    /**
     * Restarts the cache provider.
     */
    @Override
    public void restart() {
        stop();
        start();
    }

    /**
     * Returns whether this cache provider is started.
     *
     * @return Whether this cache provider is started.
     */
    @Override
    public boolean isStarted() {
        return started;
    }

    /**
     * Returns whether this cache provider is lazy.
     *
     * @return Whether this cache provider is lazy.
     */
    public boolean isLazy() {
        return lazy;
    }

    /**
     * Returns an IData representation of this object.
     *
     * @return An IData representation of this object.
     */
    @Override
    public IData getIData() {
        IData document = IDataFactory.create();
        IDataCursor cursor = document.getCursor();

        try {
            cursor.insertAfter("cache.name", getCacheName());
            cursor.insertAfter("cache.length", cache.size());
            cursor.insertAfter("cache", cacheToIData());
        } finally {
            cursor.destroy();
        }

        return document;
    }

    /**
     * Returns an IData representation of the cache.
     * @return An IData representation of the cache.
     */
    private IData cacheToIData() {
        IData document = IDataFactory.create();
        IDataCursor cursor = document.getCursor();

        try {
            for (Map.Entry<K, V> entry : cache.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();
                Object valueObject;
                if (key != null) {
                    if (value instanceof IData || value instanceof IDataCodable || value instanceof IDataPortable || value instanceof ValuesCodable || value instanceof Map) {
                        valueObject = IDataHelper.toIData(value);
                    } else {
                        valueObject = value;
                    }
                    cursor.insertAfter(key.toString(), valueObject);
                }
            }
        } finally {
            cursor.destroy();
        }

        return document;
    }

    /**
     * Not implemented.
     *
     * @param document Do not use.
     */
    @Override
    public void setIData(IData document) {
        throw new UnsupportedOperationException("setIData(IData) not implemented");
    }
}
