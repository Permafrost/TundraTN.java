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

package permafrost.tundra.tn.document;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.MessageFormat;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.db.BizDocStore;
import com.wm.app.tn.db.Datastore;
import com.wm.app.tn.db.DatastoreException;
import com.wm.app.tn.db.SQLWrappers;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.app.tn.doc.BizDocErrorSet;
import com.wm.app.tn.err.ActivityLogEntry;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.data.IDataUtil;
import com.wm.lang.ns.NSName;
import permafrost.tundra.lang.ExceptionHelper;

/**
 * A collection of convenience methods for working with Trading Networks BizDocEnvelope objects.
 */
public final class BizDocEnvelopeHelper {
    /**
     * The activity log message class that represents unrecoverable errors.
     */
    private static final String ACTIVITY_LOG_UNRECOVERABLE_MESSAGE_CLASS = "Unrecoverable";
    /**
     * The default timeout for database queries.
     */
    private static final int DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS = 30;
    /**
     * SQL statement for updating a bizdoc's user status with optimistic concurrency.
     */
    private static final String UPDATE_BIZDOC_USER_STATUS_SQL = "UPDATE BizDoc SET UserStatus = ?, LastModified = ? WHERE DocID = ? AND UserStatus = ?";
    /**
     * SQL statement for updating a bizdoc's system status with optimistic concurrency.
     */
    private static final String UPDATE_BIZDOC_SYSTEM_STATUS_SQL = "UPDATE BizDoc SET RoutingStatus = ?, LastModified = ? WHERE DocID = ? AND RoutingStatus = ?";
    /**
     * SQL statement for updating a bizdoc's system and user status with optimistic concurrency.
     */
    private static final String UPDATE_BIZDOC_STATUS_SQL = "UPDATE BizDoc SET RoutingStatus = ?, UserStatus = ?, LastModified = ? WHERE DocID = ? AND RoutingStatus = ? AND UserStatus = ?";

    /**
     * Disallow instantiation of this class.
     */
    private BizDocEnvelopeHelper() {}

    /**
     * Returns a full BizDocEnvelope, if given either a subset or full BizDocEnvelope as an IData document.
     *
     * @param input                 An IData document which could be a BizDocEnvelope, or could be a subset of a
     *                              BizDocEnvelope that includes an InternalID key.
     * @return                      The full BizDocEnvelope associated with the given IData document.
     * @throws DatastoreException   If a database error occurs.
     */
    public static BizDocEnvelope normalize(IData input) throws DatastoreException {
        return normalize(input, false);
    }

    /**
     * Returns a full BizDocEnvelope, if given either a subset or full BizDocEnvelope as an IData document.
     *
     * @param input                 An IData document which could be a BizDocEnvelope, or could be a subset of a
     *                              BizDocEnvelope that includes an InternalID key.
     * @param includeContent        Whether to include all content parts with the returned BizDocEnvelope.
     * @return                      The full BizDocEnvelope associated with the given IData document.
     * @throws DatastoreException   If a database error occurs.
     */
    public static BizDocEnvelope normalize(IData input, boolean includeContent) throws DatastoreException {
        if (input == null) return null;

        BizDocEnvelope document = null;

        if (input instanceof BizDocEnvelope) {
            document = (BizDocEnvelope)input;
            if (includeContent && document.getContent() == null) document = get(document.getInternalId(), includeContent);
        } else {
            IDataCursor cursor = input.getCursor();
            String id = IDataUtil.getString(cursor, "InternalID");
            cursor.destroy();

            if (id == null) throw new IllegalArgumentException("InternalID is required");

            document = get(id, includeContent);
        }

        return document;
    }

    /**
     * Returns the BizDocEnvelope associated with the given ID without its associated content parts.
     *
     * @param id                    The ID of the BizDocEnvelope to be returned.
     * @return                      The BizDocEnvelope associated with the given ID.
     * @throws DatastoreException   If a database exception occurs.
     */
    public static BizDocEnvelope get(String id) throws DatastoreException {
        return get(id, false);
    }

    /**
     * Returns the BizDocEnvelope, and optionally its content parts, associated with the given ID.
     *
     * @param id                    The ID of the BizDocEnvelope to be returned.
     * @param includeContent        Whether to include all content parts with the returned BizDocEnvelope.
     * @return                      The BizDocEnvelope associated with the given ID.
     * @throws DatastoreException   If a database exception occurs.
     */
    public static BizDocEnvelope get(String id, boolean includeContent) throws DatastoreException {
        if (id == null) return null;
        return BizDocStore.getDocument(id, includeContent);
    }

    /**
     * Refreshes the given BizDocEnvelope from the Trading Networks database.
     *
     * @param document              The BizDocEnvelope to be refreshed.
     * @return                      The given BizDocEnvelope refreshed from the Trading Networks database.
     * @throws DatastoreException   If a database error occurs.
     */
    public static BizDocEnvelope refresh(BizDocEnvelope document) throws DatastoreException {
        if (document == null) return null;
        return get(document.getInternalId());
    }

    /**
     * Updates the status on the given BizDocEnvelope.
     *
     * @param bizdoc            The BizDocEnvelope to update the status on.
     * @param systemStatus      The system status to be set.
     * @param userStatus        The user status to be set.
     * @throws ServiceException If a database error is encountered.
     */
    public static void setStatus(BizDocEnvelope bizdoc, String systemStatus, String userStatus) throws ServiceException {
        setStatus(bizdoc, systemStatus, userStatus, false);
    }

    /**
     * Updates the status on the given BizDocEnvelope.
     *
     * @param bizdoc            The BizDocEnvelope to update the status on.
     * @param systemStatus      The system status to be set. If null, system status will not be set.
     * @param userStatus        The user status to be set.
     * @param silence           If true, the status is not changed.
     * @throws ServiceException If a database error is encountered.
     */
    public static void setStatus(BizDocEnvelope bizdoc, String systemStatus, String userStatus, boolean silence) throws ServiceException {
        setStatus(bizdoc, systemStatus, null, userStatus, null, silence);
    }

    /**
     * Updates the status on the given BizDocEnvelope with optimistic concurrency supported by ensuring the status
     * is only updated if it equals the given previous value.
     *
     * @param bizdoc                The BizDocEnvelope to update the status on.
     * @param systemStatus          The system status to be set. If null, system status will not be set.
     * @param previousSystemStatus  The previous value of the system status.
     * @param userStatus            The user status to be set. If null, user status will not be set.
     * @param previousUserStatus    The previous value of the user status.
     * @param silence               If true, the status is not changed.
     * @throws ServiceException     If a database error is encountered.
     */
    public static void setStatus(BizDocEnvelope bizdoc, String systemStatus, String previousSystemStatus, String userStatus, String previousUserStatus, boolean silence) throws ServiceException {
        if (bizdoc == null || silence) return;

        boolean result;

        if (previousSystemStatus == null && previousUserStatus == null) {
            result = BizDocStore.changeStatus(bizdoc.getInternalId(), systemStatus, userStatus);
        } else if (systemStatus != null && userStatus != null) {
            result = setStatusForPrevious(bizdoc, systemStatus, previousSystemStatus, userStatus, previousUserStatus);
        } else if (systemStatus != null) {
            result = setSystemStatusForPrevious(bizdoc, systemStatus, previousSystemStatus);
        } else {
            result = setUserStatusForPrevious(bizdoc, userStatus, previousUserStatus);
        }

        if (result) log(bizdoc, "MESSAGE", "General", "Status changed", getStatusMessage(systemStatus, userStatus));
    }

    /**
     * Updates the status on the given BizDocEnvelope with optimistic concurrency supported by ensuring the status
     * is only updated if it equals the given previous value.
     *
     * @param bizdoc                The BizDocEnvelope to update the status on.
     * @param systemStatus          The system status to be set. If null, system status will not be set.
     * @param previousSystemStatus  The previous value of the system status.
     * @param userStatus            The user status to be set. If null, user status will not be set.
     * @param previousUserStatus    The previous value of the user status.
     * @return                      True if the status was updated.
     * @throws ServiceException     If a database error is encountered.
     */
    private static boolean setStatusForPrevious(BizDocEnvelope bizdoc, String systemStatus, String previousSystemStatus, String userStatus, String previousUserStatus) throws ServiceException {
        Connection connection = null;
        PreparedStatement statement = null;
        boolean result = false;

        try {
            connection = Datastore.getConnection();
            statement = connection.prepareStatement(UPDATE_BIZDOC_STATUS_SQL);
            statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
            statement.clearParameters();

            SQLWrappers.setChoppedString(statement, 1, systemStatus, "BizDoc.RoutingStatus");
            SQLWrappers.setChoppedString(statement, 2, userStatus, "BizDoc.UserStatus");
            SQLWrappers.setNow(statement, 3);
            SQLWrappers.setCharString(statement, 4, bizdoc.getInternalId());
            SQLWrappers.setChoppedString(statement, 5, previousSystemStatus, "BizDoc.RoutingStatus");
            SQLWrappers.setChoppedString(statement, 6, previousUserStatus, "BizDoc.UserStatus");

            result = statement.executeUpdate() == 1;

            connection.commit();
        } catch (SQLException ex) {
            connection = Datastore.handleSQLException(connection, ex);
            ExceptionHelper.raise(ex);
        } finally {
            SQLWrappers.close(statement);
            Datastore.releaseConnection(connection);
        }

        if (result) {
            bizdoc.setSystemStatus(systemStatus);
            bizdoc.setUserStatus(userStatus);
        }

        return result;
    }

    /**
     * Updates the status on the given BizDocEnvelope with optimistic concurrency supported by ensuring the status
     * is only updated if it equals the given previous value.
     *
     * @param bizdoc                The BizDocEnvelope to update the status on.
     * @param systemStatus          The system status to be set. If null, system status will not be set.
     * @param previousSystemStatus  The previous value of the system status.
     * @return                      True if the status was updated.
     * @throws ServiceException     If a database error is encountered.
     */
    private static boolean setSystemStatusForPrevious(BizDocEnvelope bizdoc, String systemStatus, String previousSystemStatus) throws ServiceException {
        Connection connection = null;
        PreparedStatement statement = null;
        boolean result = false;

        try {
            connection = Datastore.getConnection();
            statement = connection.prepareStatement(UPDATE_BIZDOC_SYSTEM_STATUS_SQL);
            statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
            statement.clearParameters();

            SQLWrappers.setChoppedString(statement, 1, systemStatus, "BizDoc.RoutingStatus");
            SQLWrappers.setNow(statement, 2);
            SQLWrappers.setCharString(statement, 3, bizdoc.getInternalId());
            SQLWrappers.setChoppedString(statement, 4, previousSystemStatus, "BizDoc.RoutingStatus");

            result = statement.executeUpdate() == 1;

            connection.commit();
        } catch (SQLException ex) {
            connection = Datastore.handleSQLException(connection, ex);
            ExceptionHelper.raise(ex);
        } finally {
            SQLWrappers.close(statement);
            Datastore.releaseConnection(connection);
        }

        if (result) bizdoc.setSystemStatus(systemStatus);

        return result;
    }

    /**
     * Updates the status on the given BizDocEnvelope with optimistic concurrency supported by ensuring the status
     * is only updated if it equals the given previous value.
     *
     * @param bizdoc                The BizDocEnvelope to update the status on.
     * @param userStatus            The user status to be set. If null, user status will not be set.
     * @param previousUserStatus    The previous value of the user status.
     * @return                      True if the status was updated.
     * @throws ServiceException     If a database error is encountered.
     */
    private static boolean setUserStatusForPrevious(BizDocEnvelope bizdoc, String userStatus, String previousUserStatus) throws ServiceException {
        Connection connection = null;
        PreparedStatement statement = null;
        boolean result = false;

        try {
            connection = Datastore.getConnection();
            statement = connection.prepareStatement(UPDATE_BIZDOC_USER_STATUS_SQL);
            statement.setQueryTimeout(DEFAULT_SQL_STATEMENT_QUERY_TIMEOUT_SECONDS);
            statement.clearParameters();

            SQLWrappers.setChoppedString(statement, 1, userStatus, "BizDoc.UserStatus");
            SQLWrappers.setNow(statement, 2);
            SQLWrappers.setCharString(statement, 3, bizdoc.getInternalId());
            SQLWrappers.setChoppedString(statement, 4, previousUserStatus, "BizDoc.UserStatus");

            result = statement.executeUpdate() == 1;

            connection.commit();
        } catch (SQLException ex) {
            connection = Datastore.handleSQLException(connection, ex);
            ExceptionHelper.raise(ex);
        } finally {
            SQLWrappers.close(statement);
            Datastore.releaseConnection(connection);
        }

        if (result) bizdoc.setUserStatus(userStatus);

        return result;
    }

    /**
     * Returns a message suitable for logging about the given status changes.
     *
     * @param systemStatus The system status that was set.
     * @param userStatus   The user status that was set.
     * @return             A message suitable for logging about the given status changes.
     */
    private static String getStatusMessage(String systemStatus, String userStatus) {
        String message = null;
        if (systemStatus != null && userStatus != null) {
            message = MessageFormat.format("System status changed to \"{0}\"; user status changed to \"{1}\"", systemStatus, userStatus);
        } else if (systemStatus != null) {
            message = MessageFormat.format("System status changed to \"{0}\"", systemStatus);
        } else if (userStatus != null) {
            message = MessageFormat.format("User status changed to \"{0}\"", userStatus);
        }
        return message;
    }

    /**
     * The implementation service for BizDocEnvelope logging.
     */
    private static final NSName LOG_SERVICE = NSName.create("tundra.tn:log");

    /**
     * Adds an activity log statement to the given BizDocEnvelope.
     * TODO: convert this to a pure java service, rather than an invoke of a flow service.
     *
     * @param bizdoc            The BizDocEnvelope to add the activity log statement to.
     * @param messageType       The type of message to be logged.
     * @param messageClass      The class of the message to be logged.
     * @param messageSummary    The summary of the message to be logged.
     * @param messageDetails    The detail of the message to be logged.
     * @throws ServiceException If an error occurs while logging.
     */
    public static void log(BizDocEnvelope bizdoc, String messageType, String messageClass, String messageSummary, String messageDetails) throws ServiceException {
        IData input = IDataFactory.create();
        IDataCursor cursor = input.getCursor();
        IDataUtil.put(cursor, "$bizdoc", bizdoc);
        IDataUtil.put(cursor, "$type", messageType);
        IDataUtil.put(cursor, "$class", messageClass);
        IDataUtil.put(cursor, "$summary", messageSummary);
        IDataUtil.put(cursor, "$message", messageDetails);
        cursor.destroy();

        try {
            Service.doInvoke(LOG_SERVICE, input);
        } catch (Exception ex) {
            ExceptionHelper.raise(ex);
        }
    }

    /**
     * Returns true if the given BizDocEnvelope has any unrecoverable errors.
     *
     * @param document              The BizDocEnvelope to check for unrecoverable errors.
     * @return                      True if the given BizDocEnvelope has unrecoverable errors.
     * @throws DatastoreException   If a database error occurs.
     */
    public static boolean hasUnrecoverableErrors(BizDocEnvelope document) throws DatastoreException {
        return hasErrors(document, ACTIVITY_LOG_UNRECOVERABLE_MESSAGE_CLASS);
    }

    /**
     * Returns true if the given BizDocEnvelope has any errors.
     *
     * @param document              The BizDocEnvelope to check for errors.
     * @return                      True if the given BizDocEnvelope has errors.
     * @throws DatastoreException   If a database error occurs.
     */
    public static boolean hasErrors(BizDocEnvelope document) throws DatastoreException {
        return hasErrors(document, null);
    }

    /**
     * Returns true if the given BizDocEnvelope has any errors of the given message class.
     *
     * @param document              The BizDocEnvelope to check for errors.
     * @param messageClass          The class of error to check for.
     * @return                      True if the given BizDocEnvelope has errors of the given class.
     * @throws DatastoreException   If a database error occurs.
     */
    public static boolean hasErrors(BizDocEnvelope document, String messageClass) throws DatastoreException {
        boolean hasErrors = false;

        if (document != null) {
            BizDocErrorSet errorSet = refresh(document).getErrorSet();

            if (errorSet != null && errorSet.getErrorCount() > 0) {
                if (messageClass == null) {
                    hasErrors = true;
                } else {
                    ActivityLogEntry[] activityLogEntries = errorSet.getErrors(messageClass);
                    hasErrors = activityLogEntries != null && activityLogEntries.length > 0;
                }
            }
        }

        return hasErrors;
    }
}
