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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.db.BizDocStore;
import com.wm.app.tn.db.Datastore;
import com.wm.app.tn.db.DatastoreException;
import com.wm.app.tn.db.SQLStatements;
import com.wm.app.tn.db.SQLWrappers;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.app.tn.doc.BizDocErrorSet;
import com.wm.app.tn.err.ActivityLogEntry;
import com.wm.app.tn.profile.ProfileSummary;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.data.IDataUtil;
import com.wm.lang.ns.NSName;
import permafrost.tundra.content.DuplicateException;
import permafrost.tundra.content.MalformedException;
import permafrost.tundra.content.StrictException;
import permafrost.tundra.content.UnsupportedException;
import permafrost.tundra.content.ValidationException;
import permafrost.tundra.lang.BooleanHelper;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.lang.StringHelper;
import permafrost.tundra.time.DateTimeHelper;
import permafrost.tundra.tn.profile.ProfileHelper;

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
     * Returns true if the given document is a duplicate of another existing document, where duplicates are defined
     * as having the same document type, sender, receiver, and document ID.
     *
     * @param document          The document to check whether it is a duplicate.
     * @return                  True if this document is a duplicate.
     * @throws ServiceException If a database error occurs.
     */
    public static boolean isDuplicate(BizDocEnvelope document) throws ServiceException {
        Connection connection = null;
        PreparedStatement statement = null;
        boolean isDuplicate = false;

        try {
            StringBuilder key = new StringBuilder();
            key.append(document.getDocType().getId());
            key.append(document.getSenderId());
            key.append(document.getReceiverId());
            key.append(document.getDocumentId());

            connection = Datastore.getConnection();

            statement = SQLStatements.prepareStatement(connection, "bdunique.insert");
            SQLWrappers.setCharString(statement, 1, document.getInternalId());
            SQLWrappers.setChoppedString(statement, 2, key.toString(), "BizDocUniqueKeys.UniqueKey");

            try {
                statement.executeUpdate();
            } catch (SQLException ex) {
                isDuplicate = true;
            }
        } catch (SQLException ex) {
            connection = Datastore.handleSQLException(connection, ex);
            ExceptionHelper.raise(ex);
        } finally {
            SQLStatements.releaseStatement(statement);
            Datastore.releaseConnection(connection);
        }

        return isDuplicate;
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
     * @return                      True if the status was updated.
     * @throws ServiceException     If a database error is encountered.
     */
    public static boolean setStatus(BizDocEnvelope bizdoc, String systemStatus, String previousSystemStatus, String userStatus, String previousUserStatus, boolean silence) throws ServiceException {
        if (bizdoc == null || silence) return false;

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

        return result;
    }

    /**
     * Updates the status on the given BizDocEnvelope with optimistic concurrency supported by ensuring the status
     * is only updated if it equals the given previous value.
     *
     * @param internalID            The internal ID of the bizdoc to update the status on.
     * @param systemStatus          The system status to be set. If null, system status will not be set.
     * @param previousSystemStatus  The previous value of the system status.
     * @param userStatus            The user status to be set. If null, user status will not be set.
     * @param previousUserStatus    The previous value of the user status.
     * @param silence               If true, the status is not changed.
     * @return                      True if the status was updated.
     * @throws ServiceException     If a database error is encountered.
     */
    public static boolean setStatus(String internalID, String systemStatus, String previousSystemStatus, String userStatus, String previousUserStatus, boolean silence) throws ServiceException {
        if (internalID == null || silence) return false;

        boolean result;

        if (previousSystemStatus == null && previousUserStatus == null) {
            result = BizDocStore.changeStatus(internalID, systemStatus, userStatus);
        } else if (systemStatus != null && userStatus != null) {
            result = setStatusForPrevious(internalID, systemStatus, previousSystemStatus, userStatus, previousUserStatus);
        } else if (systemStatus != null) {
            result = setSystemStatusForPrevious(internalID, systemStatus, previousSystemStatus);
        } else {
            result = setUserStatusForPrevious(internalID, userStatus, previousUserStatus);
        }

        if (result) log(internalID, "MESSAGE", "General", "Status changed", getStatusMessage(systemStatus, userStatus));

        return result;
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
        boolean result = setStatusForPrevious(bizdoc.getInternalId(), systemStatus, previousSystemStatus, userStatus, previousUserStatus);

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
     * @param internalID            The internal ID of the bizdoc to update the status on.
     * @param systemStatus          The system status to be set. If null, system status will not be set.
     * @param previousSystemStatus  The previous value of the system status.
     * @param userStatus            The user status to be set. If null, user status will not be set.
     * @param previousUserStatus    The previous value of the user status.
     * @return                      True if the status was updated.
     * @throws ServiceException     If a database error is encountered.
     */
    private static boolean setStatusForPrevious(String internalID, String systemStatus, String previousSystemStatus, String userStatus, String previousUserStatus) throws ServiceException {
        if (internalID == null) throw new NullPointerException("internalID must not be null");
        if (systemStatus == null) throw new NullPointerException("userStatus must not be null");
        if (previousSystemStatus == null) throw new NullPointerException("previousUserStatus must not be null");
        if (userStatus == null) throw new NullPointerException("userStatus must not be null");
        if (previousUserStatus == null) throw new NullPointerException("previousUserStatus must not be null");

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
            SQLWrappers.setCharString(statement, 4, internalID);
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
    public static boolean setSystemStatusForPrevious(BizDocEnvelope bizdoc, String systemStatus, String previousSystemStatus) throws ServiceException {
        boolean result = setSystemStatusForPrevious(bizdoc.getInternalId(), systemStatus, previousSystemStatus);
        if (result) bizdoc.setSystemStatus(systemStatus);
        return result;
    }

    /**
     * Updates the status on the given BizDocEnvelope with optimistic concurrency supported by ensuring the status
     * is only updated if it equals the given previous value.
     *
     * @param internalID            The internal ID of the bizdoc to update the status on.
     * @param systemStatus          The system status to be set. If null, system status will not be set.
     * @param previousSystemStatus  The previous value of the system status.
     * @return                      True if the status was updated.
     * @throws ServiceException     If a database error is encountered.
     */
    public static boolean setSystemStatusForPrevious(String internalID, String systemStatus, String previousSystemStatus) throws ServiceException {
        if (internalID == null) throw new NullPointerException("internalID must not be null");
        if (systemStatus == null) throw new NullPointerException("userStatus must not be null");
        if (previousSystemStatus == null) throw new NullPointerException("previousUserStatus must not be null");

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
            SQLWrappers.setCharString(statement, 3, internalID);
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
    public static boolean setUserStatusForPrevious(BizDocEnvelope bizdoc, String userStatus, String previousUserStatus) throws ServiceException {
        boolean result = setUserStatusForPrevious(bizdoc.getInternalId(), userStatus, previousUserStatus);
        if (result) bizdoc.setUserStatus(userStatus);
        return result;
    }

    /**
     * Updates the status on the given BizDocEnvelope with optimistic concurrency supported by ensuring the status
     * is only updated if it equals the given previous value.
     *
     * @param internalID            The internal ID of the bizdoc to update the status on.
     * @param userStatus            The user status to be set. If null, user status will not be set.
     * @param previousUserStatus    The previous value of the user status.
     * @return                      True if the status was updated.
     * @throws ServiceException     If a database error is encountered.
     */
    public static boolean setUserStatusForPrevious(String internalID, String userStatus, String previousUserStatus) throws ServiceException {
        if (internalID == null) throw new NullPointerException("internalID must not be null");
        if (userStatus == null) throw new NullPointerException("userStatus must not be null");
        if (previousUserStatus == null) throw new NullPointerException("previousUserStatus must not be null");

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
            SQLWrappers.setCharString(statement, 3, internalID);
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
     * Adds an activity log statement to the given BizDocEnvelope.
     * TODO: convert this to a pure java service, rather than an invoke of a flow service.
     *
     * @param internalID        The BizDocEnvelope internal ID for the bizdoc to add the activity log statement to.
     * @param messageType       The type of message to be logged.
     * @param messageClass      The class of the message to be logged.
     * @param messageSummary    The summary of the message to be logged.
     * @param messageDetails    The detail of the message to be logged.
     * @throws ServiceException If an error occurs while logging.
     */
    public static void log(String internalID, String messageType, String messageClass, String messageSummary, String messageDetails) throws ServiceException {
        IData input = IDataFactory.create();
        IDataCursor cursor = input.getCursor();

        IData bizdoc = IDataFactory.create();
        IDataCursor bizdocCursor = bizdoc.getCursor();
        bizdocCursor.insertAfter("InternalID", internalID);
        bizdocCursor.destroy();

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
        return hasErrors(document, messageClass, true);
    }

    /**
     * Returns true if the given BizDocEnvelope has any errors of the given message class.
     *
     * @param document              The BizDocEnvelope to check for errors.
     * @param messageClass          The class of error to check for.
     * @param refresh               Whether to reload the BizDocEnvelope before checking it.
     * @return                      True if the given BizDocEnvelope has errors of the given class.
     * @throws DatastoreException   If a database error occurs.
     */
    public static boolean hasErrors(BizDocEnvelope document, String messageClass, boolean refresh) throws DatastoreException {
        boolean hasErrors = false;

        if (document != null) {
            if (refresh) document = refresh(document);
            hasErrors = hasErrors(document.getErrorSet(), getMessageClassesSet(messageClass));
        }

        return hasErrors;
    }

    /**
     * Returns true if the given BizDocEnvelope has any errors of the given message class.
     *
     * @param errors                The BizDocErrorSet to check for errors.
     * @param messageClasses        One or more message classes to check for.
     * @return                      True if the given BizDocErrorSet has errors of the given class.
     */
    public static boolean hasErrors(BizDocErrorSet errors, Set<String> messageClasses) {
        if (errors != null && errors.getErrorCount() > 0) {
            if (messageClasses == null || messageClasses.size() == 0) {
                messageClasses = getMessageClasses(errors);
            }

            if (messageClasses != null) {
                for (String messageClass : messageClasses) {
                    ActivityLogEntry[] entries = errors.getErrors(messageClass);
                    if (entries != null) {
                        for (ActivityLogEntry entry : entries) {
                            if (entry != null) {
                                if (entry.getEntryType() == ActivityLogEntry.TYPE_ERROR) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        }

        return false;
    }

    /**
     * Returns all message classes that exist in the given error set.
     *
     * @param errors    The error set to get message classes from.
     * @return          All message classes that exist in the given error set.
     */
    private static Set<String> getMessageClasses(BizDocErrorSet errors) {
        Set<String> output = new HashSet<String>();

        IDataCursor cursor = errors.getCursor();
        try {
            while(cursor.next()) {
                output.add(cursor.getKey());
            }
        } finally {
            cursor.destroy();
        }

        return output;
    }

    /**
     * Returns true if the given BizDocEnvelope has any errors of the given message class.
     *
     * @param document              The BizDocEnvelope to check for errors.
     * @param messageClasses        The classes of error to check for.
     * @return                      True if the given BizDocEnvelope has errors of the given class.
     * @throws DatastoreException   If a database error occurs.
     */
    public static ActivityLogEntry[] getErrors(IData document, IData messageClasses) throws DatastoreException {
        return getErrors(normalize(document, false), getMessageClassesSet(messageClasses));
    }

    /**
     * Returns true if the given BizDocEnvelope has any errors of the given message class.
     *
     * @param document              The BizDocEnvelope to check for errors.
     * @param messageClasses        The classes of error to check for.
     * @return                      True if the given BizDocEnvelope has errors of the given class.
     * @throws DatastoreException   If a database error occurs.
     */
    public static ActivityLogEntry[] getErrors(BizDocEnvelope document, Set<String> messageClasses) throws DatastoreException {
        ActivityLogEntry[] output = null;

        if (document != null) {
            BizDocErrorSet errorSet = document.getErrorSet();
            if (errorSet != null) {
                int errorCount = errorSet.getErrorCount();
                if (errorCount > 0) {
                    List<ActivityLogEntry> errors = new ArrayList<ActivityLogEntry>(errorCount);
                    if (messageClasses == null || messageClasses.size() == 0) {
                        messageClasses = getMessageClasses(errorSet);
                    }

                    if (messageClasses != null) {
                        for (String messageClass : messageClasses) {
                            ActivityLogEntry[] entries = errorSet.getErrors(messageClass);
                            if (entries != null) {
                                for (ActivityLogEntry entry : entries) {
                                    if (entry != null) {
                                        if (entry.getEntryType() == ActivityLogEntry.TYPE_ERROR) {
                                            errors.add(entry);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if (errors.size() > 0) {
                        output = errors.toArray(new ActivityLogEntry[0]);
                    }
                }
            }
        }

        return output;
    }

    /**
     * The regular expression pattern to detect if a bizdoc error is a duplicate error.
     */
    private static final Pattern DUPLICATE_DOCUMENT_ERROR_PATTERN = Pattern.compile("^(Duplicate.+|.+unique.+)$");

    /**
     * Checks if the given document has any errors of the given message classes, and if so throws an exception to stop
     * further processing.
     *
     * @param document          The bizdoc to check.
     * @param messageClasses    The activity log message classes to check.
     * @throws ServiceException If the given bizdoc has errors logged against it with any of the given message classes.
     */
    public static void raiseIfErrors(IData document, IData messageClasses) throws ServiceException {
        raiseIfErrors(normalize(document, false), getMessageClassesSet(messageClasses));
    }

    /**
     * Checks if the given document has any errors of the given message classes, and if so throws an exception to stop
     * further processing.
     *
     * @param document          The bizdoc to check.
     * @param messageClasses    The activity log message classes to check.
     * @throws ServiceException If the given bizdoc has errors logged against it with any of the given message classes.
     */
    public static void raiseIfErrors(BizDocEnvelope document, Set<String> messageClasses) throws ServiceException {
        if (document != null) {
            if ("Unknown".equals(document.getDocType().getName())) {
                throw new UnsupportedException("Unsupported message format");
            } else {
                ActivityLogEntry[] errors = getErrors(document, messageClasses);
                if (errors != null && errors.length > 0) {
                    boolean first = true;
                    StringBuilder builder = new StringBuilder();
                    Class exceptionClass = null;

                    for (ActivityLogEntry error : errors) {
                        if (error != null) {
                            if (!first) {
                                builder.append("\n");
                            }

                            String entryClass = error.getEntryClass();
                            String briefMessage = error.getBriefMessage();
                            String fullMessage = error.getFullMessage();

                            builder.append("[");
                            builder.append(entryClass);
                            builder.append("] ");
                            builder.append(briefMessage);

                            if (fullMessage != null && !fullMessage.equals("")) {
                                builder.append(": ");
                                String[] lines = StringHelper.lines(fullMessage);
                                if (lines != null && lines.length > 0) {
                                    String firstLine = lines[0];
                                    if (firstLine != null) {
                                        builder.append(firstLine.trim());
                                    }
                                }
                            }

                            if ("Validation".equals(entryClass)) {
                                exceptionClass = ValidationException.class;
                            } else if (exceptionClass == null) {
                                if ("General".equals(entryClass)) {
                                    exceptionClass = MalformedException.class;
                                } else if ("Saving".equals(entryClass) && DUPLICATE_DOCUMENT_ERROR_PATTERN.matcher(briefMessage).matches()) {
                                    exceptionClass = DuplicateException.class;
                                }
                            }

                            first = false;
                        }
                    }

                    String message = builder.toString();

                    if (!"".equals(message)) {
                        StrictException exception;

                        if (ValidationException.class.equals(exceptionClass)) {
                            exception = new ValidationException(message);
                        } else if (MalformedException.class.equals(exceptionClass)) {
                            exception = new MalformedException(message);
                        } else if (DuplicateException.class.equals(exceptionClass)) {
                            exception = new DuplicateException(message);
                        } else {
                            exception = new StrictException(message);
                        }

                        throw exception;
                    }
                }
            }
        }
    }

    /**
     * Converts the given IData containing a set of message class names as keys and boolean flags as values to a set
     * that only includes the message class names associated with the value of true.
     *
     * @param messageClasses    The message classes to convert.
     * @return                  A set of message classes associated with the value of true.
     */
    private static Set<String> getMessageClassesSet(IData messageClasses) {
        Set<String> classes = null;
        if (messageClasses != null) {
            classes = new HashSet<String>();
            IDataCursor cursor = messageClasses.getCursor();
            try {
                while (cursor.next()) {
                    String key = cursor.getKey();
                    Object value = cursor.getValue();
                    if (value != null && BooleanHelper.normalize(value)) {
                        classes.add(key);
                    }
                }
            } finally {
                cursor.destroy();
            }
        }

        return classes == null || classes.size() == 0 ? null : classes;
    }

    /**
     * Converts the given IData containing a set of message class names as keys and boolean flags as values to a set
     * that only includes the message class names associated with the value of true.
     *
     * @param messageClasses    The message classes to convert.
     * @return                  A set of message classes associated with the value of true.
     */
    private static Set<String> getMessageClassesSet(String... messageClasses) {
        Set<String> classes = new HashSet<String>();
        if (messageClasses != null && messageClasses.length > 0) {
            for (String messageClass : messageClasses) {
                if (messageClass != null) {
                    classes.add(messageClass);
                }
            }
        }
        return classes;
    }

    /**
     * Returns a string that can be used for logging the given bizdoc.
     *
     * @param bizdoc    The bizdoc to be logged.
     * @return          A string representing the given bizdoc.
     */
    public static String toLogString(BizDocEnvelope bizdoc) {
        String output = "";
        if (bizdoc != null) {
            try {
                ProfileSummary sender = ProfileHelper.getProfileSummary(bizdoc.getSenderId());
                ProfileSummary receiver = ProfileHelper.getProfileSummary(bizdoc.getReceiverId());
                output = MessageFormat.format("'{'ID={0}, Time={1}, Type={2}, Sender={3}, Receiver={4}, DocumentID={5}'}'", bizdoc.getInternalId(), DateTimeHelper.emit(bizdoc.getTimestamp(), "datetime"), bizdoc.getDocType().getName(), sender.getDisplayName(), receiver.getDisplayName(), bizdoc.getDocumentId());
            } catch (ServiceException ex) {
                // do nothing
            }
        }
        return output;
    }
}
