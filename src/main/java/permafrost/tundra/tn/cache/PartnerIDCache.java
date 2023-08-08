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

import com.wm.app.tn.db.Datastore;
import com.wm.app.tn.db.SQLStatements;
import com.wm.app.tn.db.SQLWrappers;
import permafrost.tundra.tn.profile.ExternalID;
import permafrost.tundra.tn.profile.InternalID;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Container for PartnerID caches.
 */
public class PartnerIDCache {
    /**
     * Disallow instantiation of this class.
     */
    private PartnerIDCache() {}

    /**
     * SQL statement used to seed the external identity cache.
     */
    private static final String SELECT_EXTERNAL_IDENTITIES_SQL = "SELECT A.PartnerID, C.Type, B.ExternalID, B.SequenceNumber FROM Partner A, PartnerID B, IDType C WHERE A.PartnerID = B.InternalID AND B.IDType = C.Type AND A.Deleted = ?";
    /**
     * SQL statement used to seed the internal identity cache.
     */
    private static final String SELECT_INTERNAL_IDENTITIES_SQL = "SELECT PartnerID FROM Partner WHERE Deleted = ?";

    /**
     * Cache of IDType records mapping Type to Description.
     */
    public static class InternalToExternal extends CacheProvider<InternalID, Map<Integer, SortedSet<ExternalID>>> {
        /**
         * SQL statement used to seed the external identity cache.
         */
        private static final String SELECT_EXTERNAL_IDENTITIES_BY_PARTNERID_SQL = "SELECT C.Type, B.ExternalID, B.SequenceNumber FROM Partner A, PartnerID B, IDType C WHERE A.PartnerID = B.InternalID AND B.IDType = C.Type AND A.Deleted = ? AND A.PartnerID = ?";

        /**
         * Initialization on demand holder idiom.
         */
        private static class Holder {
            /**
             * The singleton instance of the class.
             */
            private static final InternalToExternal INSTANCE = new InternalToExternal();
        }

        /**
         * Returns the singleton instance of this class.
         *
         * @return The singleton instance of this class.
         */
        public static InternalToExternal getInstance() {
            return Holder.INSTANCE;
        }

        /**
         * Disallow instantiation of this class.
         */
        private InternalToExternal() {}

        /**
         * Returns the unique name of this cache provider.
         *
         * @return The unique name of this cache provider.
         */
        @Override
        public String getCacheName() {
            return "TundraTN.Cache.PartnerID.InternalToExternal";
        }

        /**
         * Returns the ExternalID with the given type associated with the given InternalID.
         *
         * @param internalID        The InternalID.
         * @param externalIDType    The ExternalID type.
         * @return                  The ExternalID with the given type associated with the given InternalID.
         */
        public SortedSet<ExternalID> get(InternalID internalID, int externalIDType) {
            SortedSet<ExternalID> set = null;

            Map<Integer, SortedSet<ExternalID>> externalIDs = cache.get(internalID);
            if (externalIDs != null) {
                set = externalIDs.get(externalIDType);
            }

            return set == null ? null : new TreeSet<ExternalID>(set);
        }

        /**
         * Refreshes all current cache entries from the cache's source resource such as a database.
         */
        @Override
        public void refresh() {
            seed();
        }

        /**
         * Fetches the value associated with the given key from the cache's source resource.
         *
         * @param key   The key whose value is to be fetched.
         * @return      The value associated with the given key fetched from source.
         */
        @Override
        protected Map<Integer, SortedSet<ExternalID>> fetch(InternalID key) {
            Map<Integer, SortedSet<ExternalID>> value = null;

            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;

            try {
                connection = Datastore.getConnection();
                statement = connection.prepareStatement(SELECT_EXTERNAL_IDENTITIES_BY_PARTNERID_SQL);
                statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
                statement.clearParameters();

                // do not include deleted partner profiles in the results
                statement.setBoolean(1, false);
                // set partner identity to given key
                statement.setString(2, key.getIdentity());

                resultSet = statement.executeQuery();

                while(resultSet.next()) {
                    int externalIDType = resultSet.getInt(1);
                    String externalID = resultSet.getString(2);
                    int sequence = resultSet.getInt(3);

                    if (value == null) {
                        value = new ConcurrentHashMap<Integer, SortedSet<ExternalID>>();
                    }
                    SortedSet<ExternalID> set = value.get(externalIDType);
                    if (set == null) {
                        set = new ConcurrentSkipListSet<ExternalID>();
                    }
                    set.add(new ExternalID(externalID, externalIDType, sequence));
                    value.put(externalIDType, set);
                }

                connection.commit();
            } catch (SQLException ex) {
                connection = Datastore.handleSQLException(connection, ex);
                throw new CacheException(getCacheName() + " fetch item from source failed: " + key, ex);
            } finally {
                SQLWrappers.close(resultSet);
                SQLStatements.releaseStatement(statement);
                Datastore.releaseConnection(connection);
            }

            return value;
        }

        /**
         * Fetches all keys and values from the cache's source resource.
         *
         * @return All keys and values from the cache's source resource.
         */
        @Override
        protected Map<InternalID, Map<Integer, SortedSet<ExternalID>>> fetch() {
            Map<InternalID, Map<Integer, SortedSet<ExternalID>>> results = null;

            Connection connection = null;
            PreparedStatement internalStatement = null, externalStatement = null;
            ResultSet internalResultSet = null, externalResultSet = null;

            try {
                connection = Datastore.getConnection();
                externalStatement = connection.prepareStatement(SELECT_EXTERNAL_IDENTITIES_SQL);
                externalStatement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
                externalStatement.clearParameters();

                // do not include deleted partner profiles in the results
                externalStatement.setBoolean(1, false);

                externalResultSet = externalStatement.executeQuery();

                while(externalResultSet.next()) {
                    String internalID = externalResultSet.getString(1);
                    int externalIDType = externalResultSet.getInt(2);
                    String externalID = externalResultSet.getString(3);
                    int sequence = externalResultSet.getInt(4);

                    if (results == null) {
                        results = new TreeMap<InternalID, Map<Integer, SortedSet<ExternalID>>>();
                    }
                    InternalID partnerID = new InternalID(internalID);
                    Map<Integer, SortedSet<ExternalID>> externalIDResults = results.get(partnerID);
                    if (externalIDResults == null) {
                        externalIDResults = new ConcurrentHashMap<Integer, SortedSet<ExternalID>>();
                    }
                    SortedSet<ExternalID> set = externalIDResults.get(externalIDType);
                    if (set == null) {
                        set = new ConcurrentSkipListSet<ExternalID>();
                    }
                    set.add(new ExternalID(externalID, externalIDType, sequence));
                    externalIDResults.put(externalIDType, set);
                    results.put(partnerID, externalIDResults);
                }

                // add any remaining partners that do not have any external IDs with empty mappings
                internalStatement = connection.prepareStatement(SELECT_INTERNAL_IDENTITIES_SQL);
                internalStatement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
                internalStatement.clearParameters();

                // do not include deleted partner profiles in the results
                internalStatement.setBoolean(1, false);

                internalResultSet = internalStatement.executeQuery();

                while(internalResultSet.next()) {
                    InternalID internalID = new InternalID(internalResultSet.getString(1));
                    if (results == null) {
                        results = new TreeMap<InternalID, Map<Integer, SortedSet<ExternalID>>>();
                    }
                    if (!results.containsKey(internalID)) {
                        results.put(internalID, Collections.<Integer, SortedSet<ExternalID>>emptyMap());
                    }
                }

                connection.commit();
            } catch (SQLException ex) {
                connection = Datastore.handleSQLException(connection, ex);
                throw new CacheException(getCacheName() + " fetch all from source failed", ex);
            } finally {
                SQLWrappers.close(internalResultSet);
                SQLWrappers.close(externalResultSet);
                SQLStatements.releaseStatement(internalStatement);
                SQLStatements.releaseStatement(externalStatement);
                Datastore.releaseConnection(connection);
            }

            return results;
        }
    }

    /**
     * Cache of IDType records mapping Description to Type.
     */
    public static class ExternalToInternal extends CacheProvider<ExternalID, InternalID> {
        /**
         * SQL statement used to seed the external identity cache.
         */
        private static final String SELECT_EXTERNAL_IDENTITIES_BY_EXTERNALID_SQL = "SELECT A.PartnerID FROM Partner A, PartnerID B WHERE A.PartnerID = B.InternalID AND A.Deleted = ? AND B.ExternalID = ? AND B.IDType = ?";

        /**
         * Initialization on demand holder idiom.
         */
        private static class Holder {
            /**
             * The singleton instance of the class.
             */
            private static final ExternalToInternal INSTANCE = new ExternalToInternal();
        }

        /**
         * Returns the singleton instance of this class.
         *
         * @return The singleton instance of this class.
         */
        public static ExternalToInternal getInstance() {
            return Holder.INSTANCE;
        }

        /**
         * Disallow instantiation of this class.
         */
        private ExternalToInternal() {}

        /**
         * Returns the unique name of this cache provider.
         *
         * @return The unique name of this cache provider.
         */
        @Override
        public String getCacheName() {
            return "TundraTN.Cache.PartnerID.ExternalToInternal";
        }

        /**
         * Refreshes all current cache entries from the cache's source resource such as a database.
         */
        @Override
        public void refresh() {
            seed();
        }

        /**
         * Fetches the value associated with the given key from the cache's source resource.
         *
         * @param key   The key whose value is to be fetched.
         * @return      The value associated with the given key fetched from source.
         */
        @Override
        protected InternalID fetch(ExternalID key) {
            InternalID value = null;

            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;

            try {
                connection = Datastore.getConnection();
                statement = connection.prepareStatement(SELECT_EXTERNAL_IDENTITIES_BY_EXTERNALID_SQL);
                statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
                statement.clearParameters();

                // set query input parameters
                statement.setBoolean(1, false);
                statement.setString(2, key.getIdentity());
                statement.setInt(3, key.getType());

                resultSet = statement.executeQuery();

                if (resultSet.next()) {
                    value = new InternalID(resultSet.getString(1));
                }

                connection.commit();
            } catch (SQLException ex) {
                connection = Datastore.handleSQLException(connection, ex);
                throw new CacheException(getCacheName() + " fetch item from source failed: " + key, ex);
            } finally {
                SQLWrappers.close(resultSet);
                SQLStatements.releaseStatement(statement);
                Datastore.releaseConnection(connection);
            }

            return value;
        }

        /**
         * Fetches all keys and values from the cache's source resource.
         *
         * @return All keys and values from the cache's source resource.
         */
        @Override
        protected Map<ExternalID, InternalID> fetch() {
            Map<ExternalID, InternalID> results = null;

            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;

            try {
                connection = Datastore.getConnection();
                statement = connection.prepareStatement(SELECT_EXTERNAL_IDENTITIES_SQL);
                statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
                statement.clearParameters();

                // do not include deleted partner profiles in the results
                statement.setBoolean(1, false);

                resultSet = statement.executeQuery();

                while(resultSet.next()) {
                    String internalID = resultSet.getString(1);
                    int externalIDType = resultSet.getInt(2);
                    String externalID = resultSet.getString(3);
                    int sequence = resultSet.getInt(4);

                    if (results == null) {
                        results = new TreeMap<ExternalID, InternalID>();
                    }
                    results.put(new ExternalID(externalID, externalIDType, sequence), new InternalID(internalID));
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
    }
}