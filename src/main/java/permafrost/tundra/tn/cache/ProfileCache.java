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

package permafrost.tundra.tn.cache;

import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.db.Datastore;
import com.wm.app.tn.db.SQLStatements;
import com.wm.app.tn.db.SQLWrappers;
import com.wm.app.tn.profile.Profile;
import com.wm.app.tn.profile.ProfileStore;
import com.wm.data.IData;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.tn.profile.InternalID;
import permafrost.tundra.tn.profile.ProfileHelper;
import permafrost.tundra.tn.profile.ProfileID;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A local in-memory cache of Trading Networks partner cache, to improve performance of Trading Networks
 * processes, and to avoid thread-synchronization bottlenecks in the Trading Networks partner profile
 * implementation.
 */
public class ProfileCache extends CacheProvider<String, IData> {
    /**
     * SQL statement used to seed the internal identity cache.
     */
    private static final String SELECT_INTERNAL_IDENTITIES_SQL = "SELECT PartnerID FROM Partner WHERE Deleted = ?";

    /**
     * Initialization on demand holder idiom.
     */
    private static class Holder {
        /**
         * The singleton instance of the class.
         */
        private static final ProfileCache INSTANCE = new ProfileCache();
    }

    /**
     * Returns the singleton instance of this class.
     *
     * @return The singleton instance of this class.
     */
    public static ProfileCache getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * Disallow instantiation of this class;
     */
    private ProfileCache() {}

    /**
     * Returns the unique name of this cache provider.
     *
     * @return The unique name of this cache provider.
     */
    @Override
    public String getCacheName() {
        return "TundraTN.Cache.Profile";
    }

    /**
     * Refreshes all Trading Networks partner profiles stored in the cache from the Trading Networks database.
     */
    @Override
    public void refresh() {
        for (String key : cache.keySet()) {
            IData value = fetch(key);
            if (value == null) {
                cache.remove(key);
            } else {
                cache.put(key, value);
            }
        }
    }

    /**
     * Fetches the value associated with the given key from the cache's source resource.
     *
     * @param key   The key whose value is to be fetched.
     * @return      The value associated with the given key fetched from source.
     */
    @Override
    protected IData fetch(String key) {
        IData value = null;

        try {
            Profile profile = ProfileHelper.get(key, true);
            if (profile != null) {
                value = ProfileHelper.toIData(profile);
            }
        } catch(ServiceException ex) {
            throw new CacheException(getCacheName() + " fetch item from source failed: " + key, ex);
        }

        return value;
    }

    /**
     * Fetches all keys and values from the cache's source resource.
     *
     * @return All keys and values from the cache's source resource.
     */
    @Override
    protected Map<String, IData> fetch() {
        Map<String, IData> results = null;

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            connection = Datastore.getConnection();
            statement = connection.prepareStatement(SELECT_INTERNAL_IDENTITIES_SQL);
            statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
            statement.clearParameters();

            // do not include deleted partner profiles in the results
            statement.setBoolean(1, false);

            resultSet = statement.executeQuery();

            while(resultSet.next()) {
                String key = resultSet.getString(1);
                IData value = fetch(key);
                if (value != null) {
                    if (results == null) {
                        results = new TreeMap<String, IData>();
                    }
                    results.put(key, value);
                }
            }

            connection.commit();
        } catch (SQLException ex) {
            connection = Datastore.handleSQLException(connection, ex);
            throw new CacheException(getCacheName() + " fetch all from source failed", ex);
        } finally {
            SQLWrappers.close(resultSet);
            SQLStatements.releaseStatement(statement);
            Datastore.releaseConnection(connection);
        }

        return results;
    }

    /**
     * Returns a list of all the cached Trading Networks partner profiles.
     *
     * @return                  The list of all cached Trading Networks partner profiles.
     */
    public IData[] list() {
        List<IData> output = new ArrayList<IData>(cache.size());
        for (IData profile : cache.values()) {
            output.add(IDataHelper.duplicate(profile));
        }
        return output.toArray(new IData[0]);
    }

    /**
     * Returns the Trading Networks partner profile associated with the given ID from the cache if cached, or
     * from the Trading Networks database if not cached (at which time it will be lazily cached).
     *
     * @param identity          The ID associated with the profile to be returned.
     * @return                  The profile associated with the given ID, or null if no profile exists with the given ID.
     */
    public IData get(ProfileID identity) {
        return get(identity, false);
    }

    /**
     * Returns the Trading Networks partner profile associated with the given ID from the cache if cached, or
     * from the Trading Networks database if not cached or if a refresh is requested (at which time it will be lazily
     * cached).
     *
     * @param identity          The ID associated with the profile to be returned.
     * @param refresh           If true, the profile is first reloaded from the Trading Networks database before being
     *                          returned.
     * @return                  The profile associated with the given ID, or null if no profile exists with the given ID.
     */
    public IData get(ProfileID identity, boolean refresh) {
        if (identity == null) return null;

        IData output = null;

        identity = identity.intern();
        if (identity != null) {
            output = get(identity.getIdentity(), refresh);
        }

        return output;
    }

    /**
     * Returns the Trading Networks My Enterprise profile from the cache if cached, or from the Trading Networks
     * database if not cached (at which time it will be lazily cached).
     *
     * @return                  The Trading Networks My Enterprise profile.
     */
    public IData self() {
        return self(false);
    }

    /**
     * Returns the Trading Networks My Enterprise profile from the cache if cached, or from the Trading Networks
     * database if not cached or if a refresh is requested (at which time it will be lazily cached).
     *
     * @param refresh           If true, the profile is first reloaded from the Trading Networks database before being
     *                          returned.
     * @return                  The Trading Networks My Enterprise profile.
     */
    public IData self(boolean refresh) {
        IData output = null;

        String identity = ProfileStore.getMyID();
        if (identity != null) {
            output = get(new InternalID(identity), refresh);
        }

        return output;
    }
}
