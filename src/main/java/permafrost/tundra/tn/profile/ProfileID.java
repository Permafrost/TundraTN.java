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

package permafrost.tundra.tn.profile;

import com.wm.app.tn.db.Datastore;
import com.wm.app.tn.db.SQLStatements;
import com.wm.app.tn.db.SQLWrappers;
import com.wm.app.tn.profile.LookupStore;
import com.wm.app.tn.profile.LookupStoreException;
import com.wm.app.tn.profile.ProfileStore;
import com.wm.app.tn.profile.ProfileStoreException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Represents either an internal or external Trading Networks partner profile ID.
 */
public class ProfileID {
    /**
     * The type of this ID, if it is an external ID.
     */
    protected String type;
    /**
     * The value of this ID.
     */
    protected String value;

    /**
     * Constructs a new ProfileID representing an internal ID with the given value.
     *
     * @param value The internal ID of a Trading Networks partner profile.
     */
    public ProfileID(String value) {
        this(value, null);
    }

    /**
     * Constructs a new ProfileID representing an external ID with the given type and value.
     *
     * @param value The external ID of a Trading Networks partner profile.
     * @param type  The external ID type for the given value.
     */
    public ProfileID(String value, String type) {
        this.value = value;
        this.type = type;
    }

    /**
     * Returns true if the given object is considered equivalent to this object.
     *
     * @param object    The object to compare for equivalence with.
     * @return          True if the two objects are considered equivalent.
     */
    @Override
    public boolean equals(Object object) {
        boolean result = false;

        if (object instanceof ProfileID) {
            ProfileID other = (ProfileID)object;
            result = ((this.getType() == null && other.getType() == null) || this.getType().equals(other.getType())) && ((this.getValue() == null && other.getValue() == null) || this.getValue().equals(other.getValue()));
        }

        return result;
    }

    /**
     * Returns a hash code for this object.
     *
     * @return A hash code for this object.
     */
    @Override
    public int hashCode() {
        String value = this.getValue();
        int hash = 0;

        if (value != null) {
            hash = value.hashCode();

            String type = this.getType();
            if (type != null) {
                hash = hash ^ type.hashCode(); // xor the two hashes
            }
        }

        return hash;
    }

    /**
     * Returns the external ID type, or null if it is an internal ID.
     *
     * @return The external ID type, or null if it is an internal ID.
     */
    public String getType() {
        return type;
    }

    /**
     * Returns the ID's value.
     *
     * @return The ID's value.
     */
    public String getValue() {
        return value;
    }

    /**
     * Returns true if this is an internal ID.
     *
     * @return True if this is an internal ID.
     */
    public boolean isInternal() {
        return type == null;
    }

    /**
     * Returns true if this is an external ID.
     *
     * @return True if this is an external ID
     */
    public boolean isExternal() {
        return !isInternal();
    }

    /**
     * Returns an internal ID representation of this ID, if one exists.
     *
     * @return                          An internal ID representation of this ID, if one exists.
     * @throws ProfileStoreException    If a database error occurs.
     */
    public ProfileID toInternalID() throws ProfileStoreException {
        ProfileID output = null;

        if (this.isInternal()) {
            output = this;
        } else {
            output = CACHE.get(this);
            if (output == null) {
                Integer typeID = LookupStore.getExternalIDType(this.getType());
                if (typeID == null)
                    throw new LookupStoreException("Trading Networks partner profile external ID type does not exist: " + this.getType());

                if (this.getValue() != null) {
                    String internalID = ProfileStore.getInternalID(this.getValue(), typeID);
                    if (internalID != null) {
                        output = new ProfileID(internalID);
                        CACHE.put(this, output);
                    }
                }
            }
        }

        return output;
    }

    /**
     * A local in-memory cache of all external identities and the internal identity they relate to.
     */
    private static final ConcurrentMap<ProfileID, ProfileID> CACHE = new ConcurrentHashMap<ProfileID, ProfileID>();
    /**
     * SQL statement used to seed the external identity cache.
     */
    private static final String SELECT_EXTERNAL_IDENTITIES_SQL = "SELECT A.PartnerID, C.Description, B.ExternalID FROM Partner A, PartnerID B, IDType C WHERE A.PartnerID = B.InternalID AND A.Deleted = ? AND B.IDType = C.Type";
    /**
     * The default timeout for database queries.
     */
    private static final int DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS = 30;

    /**
     * Seeds the external identity cache with all external identities known at this time.
     */
    public static void seed() throws SQLException {
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

            CACHE.clear();

            while(resultSet.next()) {
                String internalID = resultSet.getString(1);
                String externalIDType = resultSet.getString(2);
                String externalID = resultSet.getString(3);
                CACHE.put(new ProfileID(externalID, externalIDType), new ProfileID(internalID));
            }

            connection.commit();
        } catch (SQLException ex) {
            connection = Datastore.handleSQLException(connection, ex);
            throw ex;
        } finally {
            SQLWrappers.close(resultSet);
            SQLStatements.releaseStatement(statement);
            Datastore.releaseConnection(connection);
        }
    }

    // seed the external identity cache immediately
    static {
        try {
            seed();
        } catch(SQLException ex) {
            // ignore exception
        }
    }
}
