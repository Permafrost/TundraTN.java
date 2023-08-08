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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Container for IDType caches.
 */
public class IDTypeCache {
    /**
     * Disallow instantiation of this class.
     */
    private IDTypeCache() {}

    /**
     * SQL statement to select all IDType records.
     */
    private static final String SELECT_IDTYPES_SQL = "SELECT Type, Description FROM IDType";

    /**
     * Cache of IDType records mapping Type to Description.
     */
    public static class TypeToDescription extends CacheProvider<Integer, String> {
        /**
         * SQL statement to select an IDType record by Type.
         */
        private static final String SELECT_IDTYPE_BY_TYPE_SQL = "SELECT Description FROM IDType WHERE Type = ?";

        /**
         * Initialization on demand holder idiom.
         */
        private static class Holder {
            /**
             * The singleton instance of the class.
             */
            private static final TypeToDescription INSTANCE = new TypeToDescription();
        }

        /**
         * Returns the singleton instance of this class.
         *
         * @return The singleton instance of this class.
         */
        public static TypeToDescription getInstance() {
            return Holder.INSTANCE;
        }

        /**
         * Disallow instantiation of this class.
         */
        private TypeToDescription() {}

        /**
         * Returns the unique name of this cache provider.
         *
         * @return The unique name of this cache provider.
         */
        @Override
        public String getCacheName() {
            return "TundraTN.Cache.IDType.TypeToDescription";
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
        protected String fetch(Integer key) {
            String value = null;

            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;

            try {
                connection = Datastore.getConnection();
                statement = connection.prepareStatement(SELECT_IDTYPE_BY_TYPE_SQL);
                statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
                statement.clearParameters();

                // set query parameter to given key
                statement.setInt(1, key);

                resultSet = statement.executeQuery();

                if (resultSet.next()) {
                    value = resultSet.getString(1);
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
        protected Map<Integer, String> fetch() {
            Map<Integer, String> results = null;

            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;

            try {
                connection = Datastore.getConnection();
                statement = connection.prepareStatement(SELECT_IDTYPES_SQL);
                statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
                statement.clearParameters();

                resultSet = statement.executeQuery();

                while(resultSet.next()) {
                    Integer id = resultSet.getInt(1);
                    String description = resultSet.getString(2);
                    if (results == null) {
                        results = new TreeMap<Integer, String>();
                    }
                    results.put(id, description);
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

    /**
     * Cache of IDType records mapping Description to Type.
     */
    public static class DescriptionToType extends CacheProvider<String, Integer> {
        /**
         * SQL statement to select an IDType record by Type.
         */
        private static final String SELECT_IDTYPE_BY_DESCRIPTION_SQL = "SELECT Type FROM IDType WHERE Description = ?";

        /**
         * Initialization on demand holder idiom.
         */
        private static class Holder {
            /**
             * The singleton instance of the class.
             */
            private static final DescriptionToType INSTANCE = new DescriptionToType();
        }

        /**
         * Returns the singleton instance of this class.
         *
         * @return The singleton instance of this class.
         */
        public static DescriptionToType getInstance() {
            return Holder.INSTANCE;
        }

        /**
         * Disallow instantiation of this class.
         */
        private DescriptionToType() {}

        /**
         * Returns the unique name of this cache provider.
         *
         * @return The unique name of this cache provider.
         */
        @Override
        public String getCacheName() {
            return "TundraTN.Cache.IDType.DescriptionToType";
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
        protected Integer fetch(String key) {
            Integer value = null;

            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;

            try {
                connection = Datastore.getConnection();
                statement = connection.prepareStatement(SELECT_IDTYPE_BY_DESCRIPTION_SQL);
                statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
                statement.clearParameters();

                // set query parameter to given key
                statement.setString(1, key);

                resultSet = statement.executeQuery();

                if (resultSet.next()) {
                    value = resultSet.getInt(1);
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
        protected Map<String, Integer> fetch() {
            Map<String, Integer> results = null;

            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;

            try {
                connection = Datastore.getConnection();
                statement = connection.prepareStatement(SELECT_IDTYPES_SQL);
                statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
                statement.clearParameters();

                resultSet = statement.executeQuery();

                while(resultSet.next()) {
                    Integer id = resultSet.getInt(1);
                    String description = resultSet.getString(2);
                    if (results == null) {
                        results = new TreeMap<String, Integer>();
                    }
                    results.put(description, id);
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