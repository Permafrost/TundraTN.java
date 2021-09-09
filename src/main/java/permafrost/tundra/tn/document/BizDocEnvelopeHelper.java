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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import com.wm.app.b2b.server.ContentInfo;
import com.wm.app.b2b.server.InvokeState;
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.db.BDRelationshipOperations;
import com.wm.app.tn.db.BizDocStore;
import com.wm.app.tn.db.Datastore;
import com.wm.app.tn.db.DatastoreException;
import com.wm.app.tn.db.DeliveryStore;
import com.wm.app.tn.db.SQLStatements;
import com.wm.app.tn.db.SQLWrappers;
import com.wm.app.tn.delivery.DeliveryJob;
import com.wm.app.tn.delivery.DeliveryQueue;
import com.wm.app.tn.delivery.DeliveryUtils;
import com.wm.app.tn.delivery.GuaranteedJob;
import com.wm.app.tn.doc.BizDocContentPart;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.app.tn.doc.BizDocErrorSet;
import com.wm.app.tn.doc.BizDocType;
import com.wm.app.tn.doc.UnknownDocType;
import com.wm.app.tn.err.ActivityLogEntry;
import com.wm.app.tn.profile.ProfileSummary;
import com.wm.app.tn.route.PreRoutingFlags;
import com.wm.app.tn.route.RoutingRule;
import com.wm.app.tn.util.Config;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.data.IDataUtil;
import com.wm.util.tspace.Reservation;
import org.w3c.dom.Node;
import permafrost.tundra.content.ContentParser;
import permafrost.tundra.content.DuplicateException;
import permafrost.tundra.content.MalformedException;
import permafrost.tundra.content.StrictException;
import permafrost.tundra.content.UnsupportedException;
import permafrost.tundra.content.ValidationException;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.id.ULID;
import permafrost.tundra.id.UUIDHelper;
import permafrost.tundra.io.InputStreamHelper;
import permafrost.tundra.lang.BooleanHelper;
import permafrost.tundra.lang.BytesHelper;
import permafrost.tundra.lang.CharsetHelper;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.lang.ObjectHelper;
import permafrost.tundra.lang.StringHelper;
import permafrost.tundra.lang.UnrecoverableException;
import permafrost.tundra.mime.MIMEClassification;
import permafrost.tundra.mime.MIMETypeHelper;
import permafrost.tundra.security.MessageDigestHelper;
import permafrost.tundra.server.ServiceHelper;
import permafrost.tundra.time.DateTimeHelper;
import permafrost.tundra.tn.delivery.GuaranteedJobHelper;
import permafrost.tundra.tn.log.ActivityLogHelper;
import permafrost.tundra.tn.log.EntryType;
import permafrost.tundra.tn.profile.ProfileCache;
import permafrost.tundra.tn.profile.ProfileHelper;
import permafrost.tundra.tn.route.RoutingRuleHelper;
import permafrost.tundra.xml.XMLHelper;
import permafrost.tundra.xml.dom.NodeHelper;
import javax.activation.MimeType;

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
        return normalize(input, includeContent, true);
    }

    /**
     * Returns a full BizDocEnvelope, if given either a subset or full BizDocEnvelope as an IData document.
     *
     * @param input                 An IData document which could be a BizDocEnvelope, or could be a subset of a
     *                              BizDocEnvelope that includes an InternalID key.
     * @param includeContent        Whether to include all content parts with the returned BizDocEnvelope.
     * @param raiseIfMissing        If true, throws an exception if the BizDocEnvelope cannot be found.
     * @return                      The full BizDocEnvelope associated with the given IData document.
     * @throws DatastoreException   If a database error occurs.
     */
    public static BizDocEnvelope normalize(IData input, boolean includeContent, boolean raiseIfMissing) throws DatastoreException {
        if (input == null) return null;

        BizDocEnvelope document;
        String id = null;

        if (input instanceof BizDocEnvelope) {
            document = (BizDocEnvelope)input;
            if (includeContent && document.isPersisted() && document.getContentParts() == null) {
                id = document.getInternalId();
                document = get(id, includeContent);
            }
        } else {
            IDataCursor cursor = input.getCursor();
            try {
                id = IDataHelper.get(cursor, "InternalID", String.class);
                if (id == null) {
                    throw new IllegalArgumentException("InternalID is required");
                } else {
                    document = get(id, includeContent);
                }
            } finally {
                cursor.destroy();
            }
        }

        if (raiseIfMissing && document == null && id != null) throw new NullPointerException("bizdoc with InternalID " + id + " does not exist");

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
        if (document == null || !document.isPersisted()) return document;
        return get(document.getInternalId());
    }

    /**
     * Returns the BizDocEnvelope identity from the given document subset that contains an InternalID value.
     *
     * @param documentSubset    The document subset to get the identity from.
     * @return                  The internal identity of the BizDocEnvelope associated with the given document subset.
     */
    public static String getIdentity(IData documentSubset) {
        String documentIdentity = null;

        if (documentSubset != null) {
            IDataCursor cursor = documentSubset.getCursor();
            try {
                documentIdentity = IDataHelper.get(cursor, "InternalID", String.class);
            } finally {
                cursor.destroy();
            }
        }

        return documentIdentity;
    }

    /**
     * The regular expression pattern used to identify derivative relationships.
     */
    private static final Pattern DERIVATIVE_RELATIONSHIP_PATTERN = Pattern.compile("Derivative:.*");

    /**
     * Returns the BizDocEnvelope derived from the given document with the given sender and receiver, if it exists.
     *
     * @param originalDocumentID    The original document's internal identity whose derivative is to be returned.
     * @param derivedSenderID       The derivative's sender profile internal identity.
     * @param derivedReceiverID     The derivative's receiver profile internal identity.
     * @return                      The derivative BizDocEnvelope, if it exists.
     * @throws DatastoreException   If a database error occurs.
     */
    public static BizDocEnvelope getDerivative(String originalDocumentID, String derivedSenderID, String derivedReceiverID) throws DatastoreException {
        BizDocEnvelope derivedDocument = null;

        if (originalDocumentID != null && derivedSenderID != null && derivedReceiverID != null) {
            IData relationships = BizDocStore.getRelatedDocuments(originalDocumentID, null);
            if (relationships != null) {
                IDataCursor cursor = relationships.getCursor();
                try {
                    while(cursor.next()) {
                        String relationship = cursor.getKey();
                        String[] documentIdentities = (String[])cursor.getValue();
                        if (relationship != null && DERIVATIVE_RELATIONSHIP_PATTERN.matcher(relationship).matches()) {
                            if (documentIdentities != null && documentIdentities.length > 1 && documentIdentities[1] != null) {
                                BizDocEnvelope candidateDocument = get(documentIdentities[1]);
                                if (candidateDocument != null && derivedSenderID.equals(candidateDocument.getSenderId()) && derivedReceiverID.equals(candidateDocument.getReceiverId())) {
                                    derivedDocument = candidateDocument;
                                    break;
                                }
                            }
                        }
                    }
                } finally {
                    cursor.destroy();
                }
            }
        }

        return derivedDocument;
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
        boolean isDuplicate = false;

        if (document != null) {
            Connection connection = null;
            PreparedStatement statement = null;


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

        boolean result = false;

        if ("DONE".equals(userStatus) && hasErrors(bizdoc)) {
            userStatus = userStatus + " W/ ERRORS";
        }

        if (previousSystemStatus == null && previousUserStatus == null) {
            if (bizdoc.isPersisted()) {
                result = BizDocStore.changeStatus(bizdoc.getInternalId(), systemStatus, userStatus);
            } else {
                result = true;
            }
            if (result) {
                if (systemStatus != null) bizdoc.setSystemStatus(systemStatus);
                if (userStatus != null) bizdoc.setUserStatus(userStatus);
            }
        } else if (systemStatus != null && userStatus != null) {
            result = setStatusForPrevious(bizdoc, systemStatus, previousSystemStatus, userStatus, previousUserStatus);
        } else if (systemStatus != null) {
            result = setSystemStatusForPrevious(bizdoc, systemStatus, previousSystemStatus);
        } else {
            result = setUserStatusForPrevious(bizdoc, userStatus, previousUserStatus);
        }

        if (result) {
            ActivityLogHelper.log(EntryType.normalize("MESSAGE"), "General", "Status changed", getStatusMessage(systemStatus, userStatus), bizdoc);
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
     * @param silence               If true, the status is not changed.
     * @return                      True if the status was updated.
     * @throws ServiceException     If a database error is encountered.
     */
    public static boolean setStatus(String internalID, String systemStatus, String previousSystemStatus, String userStatus, String previousUserStatus, boolean silence) throws ServiceException {
        return setStatus(get(internalID), systemStatus, previousSystemStatus, userStatus, previousUserStatus, silence);
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
        boolean result = false;

        if (bizdoc.isPersisted()) {
            result = setStatusForPrevious(bizdoc.getInternalId(), systemStatus, previousSystemStatus, userStatus, previousUserStatus);
            if (result) {
                bizdoc.setSystemStatus(systemStatus);
                bizdoc.setUserStatus(userStatus);
            }
        } else if ((previousSystemStatus == null || previousSystemStatus.equals(bizdoc.getSystemStatus())) && (previousUserStatus == null || previousUserStatus.equals(bizdoc.getUserStatus()))) {
            bizdoc.setSystemStatus(systemStatus);
            bizdoc.setUserStatus(userStatus);
            result = true;
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
        boolean result = false;

        if (bizdoc.isPersisted()) {
            result = setSystemStatusForPrevious(bizdoc.getInternalId(), systemStatus, previousSystemStatus);
            if (result) bizdoc.setSystemStatus(systemStatus);
        } else if (previousSystemStatus == null || previousSystemStatus.equals(bizdoc.getSystemStatus())) {
            bizdoc.setSystemStatus(systemStatus);
            result = true;
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
        boolean result = false;

        if (bizdoc.isPersisted()) {
            result = setUserStatusForPrevious(bizdoc.getInternalId(), userStatus, previousUserStatus);
            if (result) bizdoc.setUserStatus(userStatus);
        } else if (previousUserStatus == null || previousUserStatus.equals(bizdoc.getUserStatus())) {
            bizdoc.setUserStatus(userStatus);
            result = true;
        }

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
            message = MessageFormat.format("System status changed to {0}; user status changed to {1}", systemStatus, userStatus);
        } else if (systemStatus != null) {
            message = MessageFormat.format("System status changed to {0}", systemStatus);
        } else if (userStatus != null) {
            message = MessageFormat.format("User status changed to {0}", userStatus);
        }
        return message;
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
        return getErrors(normalize(document, false, false), getMessageClassesSet(messageClasses));
    }

    /**
     * The default set of ActivityLog error message classes.
     */
    private static final Set<String> DEFAULT_ERROR_MESSAGE_CLASSES;

    static {
        DEFAULT_ERROR_MESSAGE_CLASSES = new TreeSet<String>();
        DEFAULT_ERROR_MESSAGE_CLASSES.add("Security");
        DEFAULT_ERROR_MESSAGE_CLASSES.add("Recognition");
        DEFAULT_ERROR_MESSAGE_CLASSES.add("Verification");
        DEFAULT_ERROR_MESSAGE_CLASSES.add("Validation");
        DEFAULT_ERROR_MESSAGE_CLASSES.add("Persistence");
        DEFAULT_ERROR_MESSAGE_CLASSES.add("Saving");
        DEFAULT_ERROR_MESSAGE_CLASSES.add("Routing");
        DEFAULT_ERROR_MESSAGE_CLASSES.add("General");
        DEFAULT_ERROR_MESSAGE_CLASSES.add("Processing");
        DEFAULT_ERROR_MESSAGE_CLASSES.add("Delivery");
        DEFAULT_ERROR_MESSAGE_CLASSES.add("Transient");
        DEFAULT_ERROR_MESSAGE_CLASSES.add("Unrecoverable");
    }

    /**
     * Returns true if the given BizDocEnvelope has any errors.
     *
     * @param document              The BizDocEnvelope to check for errors.
     * @return                      True if the given BizDocEnvelope has errors.
     * @throws DatastoreException   If a database error occurs.
     */
    public static ActivityLogEntry[] getErrors(BizDocEnvelope document) throws DatastoreException {
        return getErrors(document, DEFAULT_ERROR_MESSAGE_CLASSES);
    }

    /**
     * Returns the list of errors from the given BizDocEnvelope with the given message class.
     *
     * @param document              The BizDocEnvelope to check for errors.
     * @param messageClasses        The classes of error to check for.
     * @return                      True if the given BizDocEnvelope has errors of the given class.
     * @throws DatastoreException   List of errors from the given BizDocEnvelope with the given message class.
     */
    public static ActivityLogEntry[] getErrors(BizDocEnvelope document, Set<String> messageClasses) throws DatastoreException {
        return getErrors(document.getErrorSet(), messageClasses);
    }

    /**
     * Returns all error logs with the given message classes from the given BizDocErrorSet object.
     *
     * @param errorSet          The BizDocErrorSet object.
     * @param messageClasses    The set of message classes.
     * @return                  The list of errors from the given BizDocErrorSet object with the given message class, or
     *                          null if there are no errors.
     */
    private static ActivityLogEntry[] getErrors(BizDocErrorSet errorSet, Set<String> messageClasses) {
        if (errorSet == null) return null;

        if (messageClasses == null || messageClasses.size() == 0) {
            messageClasses = getMessageClasses(errorSet);
        }

        List<ActivityLogEntry> errors = Collections.emptyList();

        int errorCount = getErrorCount(errorSet);
        if (errorCount > 0) {
            errors = new ArrayList<ActivityLogEntry>(errorCount);
            IDataCursor cursor = errorSet.getCursor();
            try {
                for (String messageClass : messageClasses) {
                    IData[] entries = IDataUtil.getIDataArray(cursor, messageClass);
                    if (entries != null && entries.length > 0) {
                        for (IData entry : entries) {
                            if (isActivityLogError(entry)) {
                                errors.add(toActivityLogEntry(entry));
                            }
                        }
                    }
                }
            } finally {
                cursor.destroy();
            }
        }

        return errors.size() == 0 ? null : errors.toArray(new ActivityLogEntry[0]);
    }

    /**
     * Returns the number of errors in the given BizDocErrorSet object.
     *
     * @param errorSet  The BizDocErrorSet object.
     * @return          The number of errors in the BizDocErrorSet object.
     */
    private static int getErrorCount(BizDocErrorSet errorSet) {
        int errorCount = 0;

        if (errorSet != null) {
            IDataCursor cursor = errorSet.getCursor();
            try {
                while (cursor.next()) {
                    Object value = cursor.getValue();
                    if (value instanceof IData[]) {
                        IData[] activityLogs = (IData[]) value;
                        for (IData activityLog : activityLogs) {
                            if (isActivityLogError(activityLog)) {
                                errorCount++;
                            }
                        }
                    }
                }
            } finally {
                cursor.destroy();
            }
        }

        return errorCount;
    }

    /**
     * Returns true if the given activity log represents an error.
     *
     * @param activityLog   The activity log to check.
     * @return              True if the given activity log is an error.
     */
    private static boolean isActivityLogError(IData activityLog) {
        boolean isError = false;

        if (activityLog != null) {
            IDataCursor cursor = activityLog.getCursor();
            try {
                String entryType = IDataUtil.getString(cursor, "EntryType");
                isError = Integer.toString(ActivityLogEntry.TYPE_ERROR).equals(entryType);
            } finally {
                cursor.destroy();
            }
        }

        return isError;
    }

    /**
     * Normalizes the given IData document as an ActivityLogEntry object.
     *
     * @param document  The IData document.
     * @return          The given document as an ActivityLogEntry object.
     */
    private static ActivityLogEntry toActivityLogEntry(IData document) {
        ActivityLogEntry entry;

        if (document instanceof ActivityLogEntry) {
            entry = (ActivityLogEntry)document;
        } else {
            entry = new ActivityLogEntry();
            IDataHelper.mergeInto(entry, document);
        }

        return entry;
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
        raiseIfErrors(normalize(document, false, false), getMessageClassesSet(messageClasses));
    }

    /**
     * Checks if the given document has any errors, and if so throws an exception to stop further processing.
     *
     * @param document          The bizdoc to check.
     * @throws ServiceException If the given bizdoc has errors logged against it.
     */
    public static void raiseIfErrors(BizDocEnvelope document) throws ServiceException {
        raiseIfErrors(document, DEFAULT_ERROR_MESSAGE_CLASSES);
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
     * Returns a summary of the given object, suitable for logging.
     *
     * @param object    The object to summarize.
     * @return          The summary of the object.
     */
    public static IData summarize(BizDocEnvelope object) {
        IData summary = null;
        if (object != null) {
            summary = IDataFactory.create();
            IDataCursor cursor = summary.getCursor();
            try {
                IDataHelper.put(cursor, "InternalID", object.getInternalId());
                IDataHelper.put(cursor, "DocumentID", object.getDocumentId(), false);
                IDataHelper.put(cursor, "DocTimestamp", DateTimeHelper.emit(object.getTimestamp(), "datetime"), false);
                IDataHelper.put(cursor, "DocType", BizDocTypeHelper.summarize(object.getDocType()), false);
                IDataHelper.put(cursor, "Sender", ProfileHelper.summarize(object.getSenderId()), false);
                IDataHelper.put(cursor, "Receiver", ProfileHelper.summarize(object.getReceiverId()), false);
            } finally {
                cursor.destroy();
            }
        }
        return summary;
    }

    /**
     * Recognizes the given content returning a BizDocEnvelope reading for routing to Trading Networks.
     *
     * @param content           The content to be recognized.
     * @param contentIdentity   The type of identity to be assigned to the resulting BizDocEnvelope if required.
     * @param contentNamespace  The XML namespace prefixes and URIs used when serializing content if it is specified as an IData.
     * @param parameters        The TN_parms routing hints used when recognizing the content.
     * @param pipeline          Optional pipeline containing arbitrarily specified input variables.
     * @return                  A BizDocEnvelope representing the given content ready for routing to Trading Networks.
     * @throws ServiceException If a recognition error occurs.
     */
    public static BizDocEnvelope recognize(Object content, String contentIdentity, IData contentNamespace, IData parameters, IData pipeline) throws ServiceException {
        BizDocEnvelope bizdoc = null;

        if (content != null) {
            InputStream inputStream;

            if (pipeline == null) {
                pipeline = IDataFactory.create();
            } else {
                pipeline = IDataHelper.duplicate(pipeline);
            }
            IDataCursor pipelineCursor = pipeline.getCursor();

            parameters = IDataHelper.returnOrCreate(parameters);
            IDataCursor parameterCursor = parameters.getCursor();

            // clear the content info from the invoke state as we don't want it to influence subsequent recognitions
            // that occur after an initial receive
            InvokeState currentInvokeState = InvokeState.getCurrentState();
            ContentInfo originalContentInfo = currentInvokeState.getContentInfo();
            currentInvokeState.setContentInfo(null);

            try {
                String defaultIdentity = null;
                MimeType contentType = IDataHelper.get(parameterCursor, "$contentType", MimeType.class);
                Charset contentEncoding = IDataHelper.get(parameterCursor, "$contentEncoding", Charset.class);
                String contentSchema = IDataHelper.get(parameterCursor, "$contentSchema", String.class);
                Integer contentLength = IDataHelper.get(parameterCursor, "$contentLength", Integer.class);

                // convert given content to an InputStream if required
                if (content instanceof InputStream || content instanceof byte[] || content instanceof String) {
                    inputStream = InputStreamHelper.normalize(content, contentEncoding);
                } else if (content instanceof Node) {
                    // serialize org.w3c.dom.Node object to an InputStream
                    inputStream = NodeHelper.emit((Node)content, contentEncoding);
                } else if (content instanceof IData) {
                    // serialize IData document to an InputStream using the specified schema
                    ContentParser parser = new ContentParser(contentType, contentEncoding, contentSchema, contentNamespace, false, pipeline);
                    inputStream = parser.emit((IData)content);
                    contentEncoding = parser.getCharset();
                } else if (ObjectHelper.instance(content, "com.sap.conn.idoc.IDocDocumentList")) {
                    // serialize IDocDocumentList to an InputStream containing IDoc XML
                    IData scope = IDataFactory.create();
                    IDataCursor scopeCursor = scope.getCursor();
                    try {
                        IDataHelper.put(scopeCursor, "iDocList", content);
                        IDataHelper.put(scopeCursor, "conformsTo", contentSchema);
                        scopeCursor.destroy();

                        scope = ServiceHelper.invoke("pub.sap.idoc:iDocToDocument", scope);

                        scopeCursor = scope.getCursor();
                        IData document = IDataHelper.get(scopeCursor, "document", IData.class);

                        ContentParser parser = new ContentParser(contentType, contentEncoding, contentSchema, contentNamespace, false, pipeline);
                        inputStream = parser.emit(document);
                        contentEncoding = parser.getCharset();
                    } finally {
                        scopeCursor.destroy();
                    }
                } else {
                    inputStream = InputStreamHelper.normalize(content, contentEncoding);
                }

                byte[] bytes = null;

                // calculate content length if not provided in TN_parms/$contentLength
                if (contentLength == null) {
                    bytes = BytesHelper.normalize(inputStream);
                    if (bytes != null) contentLength = bytes.length;
                    IDataHelper.put(parameterCursor, "$contentLength", contentLength, String.class,false);
                }

                // if using a message digest for the bizdoc DocumentID, calculate digest before content InputStream
                // is consumed by the recognition process
                if (!(contentIdentity == null || "UUID".equals(contentIdentity) || "ULID".equals(contentIdentity))) {
                    if (bytes == null) bytes = BytesHelper.normalize(inputStream);
                    byte[] digest = MessageDigestHelper.digest(MessageDigestHelper.normalize(contentIdentity), bytes);
                    defaultIdentity = BytesHelper.base64Encode(digest);
                }

                if (contentLength != null && contentLength > 0) {
                    // normalize MIME media type (content type) and character set (encoding)
                    if (contentType == null) {
                        if (bytes == null) bytes = BytesHelper.normalize(inputStream);

                        // if content is valid XML, then set content type to text/xml
                        String[] errors = XMLHelper.validate(new ByteArrayInputStream(bytes), contentEncoding, null, null ,false);
                        if (errors == null) {
                            contentType = MIMETypeHelper.of("text/xml");
                        } else {
                            contentType = MIMETypeHelper.getDefault();
                        }
                        IDataHelper.put(parameterCursor, "$contentType", contentType.toString());
                    } else {
                        if (contentEncoding == null) {
                            contentEncoding = CharsetHelper.of(contentType.getParameter("charset"));
                        }
                        contentType.removeParameter("charset");
                        IDataHelper.put(parameterCursor, "$contentType", contentType.toString());
                    }

                    contentEncoding = CharsetHelper.normalize(contentEncoding, contentType, MIMETypeHelper.isText(contentType), null);

                    if (bytes != null) inputStream = InputStreamHelper.normalize(bytes);

                    // provide the content type and encoding to handleLargeDoc via the current invoke state content info
                    if (contentType != null) {
                        ContentInfo contentInfo = new ContentInfo(contentType.toString());
                        if (contentEncoding != null) contentInfo.setContentEncoding(contentEncoding.name());
                        currentInvokeState.setContentInfo(contentInfo);
                    }

                    // invoke wm.tn.doc:handleLargeDoc so that large content is handled appropriately
                    IDataHelper.put(pipelineCursor, "inputStream", inputStream);
                    IDataHelper.put(pipelineCursor, "content-type", contentType.toString());
                    IDataHelper.put(pipelineCursor, "content-length", contentLength, String.class);
                    pipelineCursor.destroy();

                    pipeline = ServiceHelper.invoke("wm.tn.doc:handleLargeDoc", pipeline);

                    pipelineCursor = pipeline.getCursor();
                    IDataHelper.remove(pipelineCursor, "inputStream");
                    IDataHelper.remove(pipelineCursor, "content-type");
                    IDataHelper.remove(pipelineCursor, "content-length");

                    content = IDataHelper.first(pipelineCursor, Object.class, "node", "$reservation", "stream", "contentStream", "ffdata", "content", "jsonStream");

                    IDataHelper.remove(pipelineCursor, "stream");
                    IDataHelper.remove(pipelineCursor, "contentStream");
                    IDataHelper.remove(pipelineCursor, "ffdata");
                    IDataHelper.remove(pipelineCursor, "content");
                    IDataHelper.remove(pipelineCursor, "jsonStream");

                    MIMEClassification classification = MIMETypeHelper.classify(contentType, contentSchema);

                    if (contentEncoding != null) IDataHelper.put(parameterCursor, "$contentEncoding", contentEncoding.name());
                    if (contentType != null) {
                        contentType.removeParameter("charset");
                        IDataHelper.put(parameterCursor, "$contentType", contentType.toString());
                    }

                    // invoke wm.tn.doc:recognize to recognize the content
                    if (!(content instanceof Node || content instanceof Reservation)) {
                        InputStream contentStream = InputStreamHelper.normalize(content, contentEncoding);
                        if (classification == MIMEClassification.XML) {
                            IData scope = IDataFactory.create();
                            IDataCursor scopeCursor = scope.getCursor();
                            try {
                                IDataHelper.put(scopeCursor, "$filestream", contentStream);
                                IDataHelper.put(scopeCursor, "encoding", contentEncoding.displayName());
                                IDataHelper.put(scopeCursor, "isXML", "true");
                                scopeCursor.destroy();

                                scope = ServiceHelper.invoke("pub.xml:xmlStringToXMLNode", scope);

                                scopeCursor = scope.getCursor();
                                content = IDataHelper.get(scopeCursor, "node", Node.class);

                                IDataHelper.put(pipelineCursor, "node", content, false);
                            } finally {
                                scopeCursor.destroy();
                            }
                        } else {
                            IDataHelper.put(pipelineCursor, "ffdata", contentStream);
                        }
                    }

                    IDataHelper.put(pipelineCursor, "TN_parms", parameters, false);
                    pipelineCursor.destroy();

                    pipeline = ServiceHelper.invoke("wm.tn.doc:recognize", pipeline);

                    pipelineCursor = pipeline.getCursor();
                    bizdoc = IDataHelper.get(pipelineCursor, "bizdoc", BizDocEnvelope.class);

                    if (bizdoc != null) {
                        BizDocContentPart[] contentParts = bizdoc.getContentParts();
                        if (contentParts != null) {
                            // handle large document content correctly if wm.tn.doc:recognize did not
                            int largeDocThreshold = Config.getLargeDocThreshold();
                            if (largeDocThreshold > 0) {
                                for (BizDocContentPart contentPart : contentParts) {
                                    if (contentPart != null) {
                                        int length = contentPart.getLength();
                                        if (!contentPart.isLargePart() && length > largeDocThreshold) {
                                            InputStream contentPartStream = InputStreamHelper.normalize(contentPart.getContent());
                                            if (contentPartStream != null) {
                                                MimeType contentPartMimeType = MIMETypeHelper.normalize(MIMETypeHelper.of(contentPart.getMimeType()));
                                                Charset contentPartEncoding = CharsetHelper.normalize(contentPartMimeType.getParameter("charset"));
                                                contentPartMimeType.removeParameter("charset");
                                                if (!MIMETypeHelper.isText(contentPartMimeType))
                                                    contentPartEncoding = null;

                                                BizDocContentHelper.addContentPart(bizdoc, contentPart.getPartName(), contentPartMimeType.toString(), contentPartEncoding, contentPartStream, true);
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // if document was unknown, save the raw original content to a new content part
                        if (bytes != null && bizdoc.getDocType() instanceof UnknownDocType) {
                            BizDocContentPart bytesContentPart = BizDocContentHelper.getContentPart(bizdoc, "bytes");
                            if (bytesContentPart != null) {
                                String bytesContentPartType = bytesContentPart.getMimeType();
                                if (contentType != null && "application/x-wmidatabin".equals(MIMETypeHelper.of(bytesContentPartType).getBaseType())) {
                                    BizDocContentHelper.addContentPart(bizdoc, "content", contentType.toString(), contentEncoding, InputStreamHelper.normalize(bytes), true);
                                }
                            }
                        }

                        // assign a new DocumentID if bizdoc doesn't have one already
                        if (bizdoc.getDocumentId() == null) {
                            if (defaultIdentity == null) {
                                if ("ULID".equals(contentIdentity)) {
                                    defaultIdentity = ULID.generate();
                                } else {
                                    defaultIdentity = UUIDHelper.generate();
                                }
                            }
                            if (defaultIdentity != null) {
                                bizdoc.setDocumentId(defaultIdentity);
                            }
                        }
                    }
                }
            } catch(IOException ex) {
                ExceptionHelper.raise(ex);
            } catch(NoSuchAlgorithmException ex) {
                ExceptionHelper.raise(ex);
            } finally {
                pipelineCursor.destroy();
                parameterCursor.destroy();
                currentInvokeState.setContentInfo(originalContentInfo);
            }
        }

        return bizdoc;
    }

    /**
     * Routes the given content to Trading Networks as a new BizDocEnvelope document.
     *
     * @param content           The content to be routed.
     * @param contentIdentity   The type of document identity to assign to the BizDocEnvelope if none is extracted.
     * @param contentType       The MIME media type of the given content.
     * @param contentEncoding   The character set used to encode the given content.
     * @param contentNamespace  The namespace prefixes and URIs required to parse the given content, if XML.
     * @param contentSchema     The content schema used to parse the given content.
     * @param attributes        The attributes to be set on the resulting BizDocEnvelope.
     * @param parameters        The Trading Networks routing hints used while routing.
     * @param pipeline          The pipeline against which variable substitution is resolved.
     * @param strict            Whether to abort routing if there are errors on the recognized BizDocEnvelope.
     * @return                  The newly routed BizDocEnvelope.
     * @throws ServiceException If an error occurs during routing.
     */
    public static BizDocEnvelope route(Object content, String contentIdentity, MimeType contentType, Charset contentEncoding, IData contentNamespace, String contentSchema, IData attributes, IData parameters, IData pipeline, boolean strict) throws ServiceException {
        long startTime = System.nanoTime();

        BizDocEnvelope bizdoc = null;

        if (content != null) {
            parameters = IDataHelper.returnOrCreate(parameters);
            IDataCursor parameterCursor = parameters.getCursor();

            try {
                contentEncoding = CharsetHelper.normalize(contentEncoding, contentType, true, IDataHelper.getOrDefault(parameterCursor, "$contentEncoding", Charset.class, CharsetHelper.DEFAULT_CHARSET));
                if (contentType != null) IDataHelper.put(parameterCursor, "$contentType", contentType.toString(), false);
                if (contentEncoding != null) IDataHelper.put(parameterCursor, "$contentEncoding", contentEncoding.displayName());
                IDataHelper.put(parameterCursor, "$contentSchema", contentSchema, false);
            } finally {
                parameterCursor.destroy();
            }

            bizdoc = recognize(content, contentIdentity, contentNamespace, parameters, pipeline);

            if (bizdoc != null) {
                try {
                    BizDocAttributeHelper.merge(bizdoc, attributes, pipeline, true);
                } catch(ServiceException ex) {
                    // ignore exception
                }

                route(bizdoc, false, null, parameters, strict);

                ActivityLogHelper.log(EntryType.MESSAGE, "General", "Document routed by " + ServiceHelper.getInitiator(), null, bizdoc, startTime, System.nanoTime());
            }
        }

        return bizdoc;
    }

    /**
     * Relates two BizDocEnvelopes to each other.
     *
     * @param source            The source BizDocEnvelope.
     * @param target            The target BizDocEnvelope.
     * @param relationship      The relationship between the BizDocEnvelopes.
     * @throws ServiceException If an error occurs.
     */
    public static void relate(BizDocEnvelope source, BizDocEnvelope target, String relationship) throws ServiceException {
        if (source == null || target == null) return;
        if (relationship == null) relationship = "Unknown";

        long startTime = System.nanoTime();

        try {
            if (source.isPersisted() && target.isPersisted()) {
                BDRelationshipOperations.relate(source.getInternalId(), target.getInternalId(), relationship);
            } else if (source.isPersisted()) {
                target.addRelationship(source.getInternalId(), target.getInternalId(), relationship);
            } else {
                source.addRelationship(source.getInternalId(), target.getInternalId(), relationship);
            }

            double duration = (System.nanoTime() - startTime) / 1000000000.0;
            ActivityLogHelper.log(EntryType.MESSAGE, "General", "Document related to " + target.getInternalId(), "Document related to " + target.getInternalId() + ": " + relationship, source, duration);
            ActivityLogHelper.log(EntryType.MESSAGE, "General", "Document related from " + source.getInternalId(), "Document related from " + source.getInternalId() + ": " + relationship, target, duration);
        } catch(SQLIntegrityConstraintViolationException ex) {
            // ignore this exception, as these two documents are already related
        } catch(SQLException ex) {
            ExceptionHelper.raise(ex);
        }
    }

    /**
     * Reroutes the given BizDocEnvelope in Trading Networks.
     *
     * @param document              The BizDocEnvelope to reroute.
     * @param parameters            The Trading Networks routing hints to use when routing.
     * @throws ServiceException     If an error occurs during routing.
     */
    public static void reroute(BizDocEnvelope document, IData parameters) throws ServiceException {
        if (document != null) {
            RoutingRule rule = RoutingRuleHelper.match(document, parameters, true);
            RoutingRuleHelper.execute(rule, document, parameters);
        }
    }

    /**
     * Routes the given BizDocEnvelope to Trading Networks.
     *
     * @param document              The BizDocEnvelope to route.
     * @param transportLog          Whether to log the transport info as a content part.
     * @param transportLogPartName  The content part name for the transport info log.
     * @param parameters            The Trading Networks routing hints to use when routing.
     * @param strict                Whether to abort routing if errors exist on the BizDocEnvelope.
     * @throws ServiceException     If an error occurs during routing.
     */
    public static void route(BizDocEnvelope document, boolean transportLog, String transportLogPartName, IData parameters, boolean strict) throws ServiceException {
        if (document != null) {
            applyParameters(document, parameters);
            normalizeContentType(document);
            RoutingRule rule = RoutingRuleHelper.match(document, parameters, true);
            persist(document, rule, transportLog, transportLogPartName, strict);
            RoutingRuleHelper.execute(rule, document, parameters);
        }
    }

    /**
     * Normalizes the each of the given BizDocEnvelope's BizDocContentPart's MIME content type: if null the default
     * content type is returned, otherwise if the content type contains the Trading Networks default charset parameter
     * of "UTF8", it is updated to have the correct value of "UTF-8".
     *
     * @param document              The BizDocEnvelope with content parts whose content types are to be normalized.
     */
    private static void normalizeContentType(BizDocEnvelope document) {
        if (document != null) {
            normalizeContentType(document.getContentParts());
        }
    }

    /**
     * Normalizes the each of the given BizDocEnvelope's BizDocContentPart's MIME content type: if null the default
     * content type is returned, otherwise if the content type contains the Trading Networks default charset parameter
     * of "UTF8", it is updated to have the correct value of "UTF-8".
     *
     * @param contentParts      The BizDocContentPart[] containing content parts whose content type is to be normalized.
     */
    private static void normalizeContentType(BizDocContentPart[] contentParts) {
        if (contentParts != null) {
            // normalize content type on each content part
            for (BizDocContentPart contentPart : contentParts) {
                normalizeContentType(contentPart);
            }
        }
    }

    /**
     * Normalizes the the given BizDocContentPart's MIME content type: if null the default content type is returned,
     * otherwise if the content type contains the Trading Networks default charset parameter of "UTF8", it is updated to
     * have the correct value of "UTF-8".
     *
     * @param contentPart       The BizDocContentPart whose content type is to be normalized.
     */
    private static void normalizeContentType(BizDocContentPart contentPart) {
        if (contentPart != null) {
           contentPart.setMimeType(BizDocContentHelper.normalizeContentType(contentPart.getMimeType()));
        }
    }

    /**
     * Applies the Trading Networks routing hint parameters to the given BizDocEnvelope document.
     *
     * @param document          The BizDocEnvelope to apply the given parameters to.
     * @param parameters        The routing hint parameters to apply.
     * @throws ServiceException If a database error occurs.
     */
    private static void applyParameters(BizDocEnvelope document, IData parameters) throws ServiceException {
        if (document == null || IDataHelper.size(parameters) == 0) return;

        IDataCursor parameterCursor = parameters.getCursor();
        try {
            String documentID = IDataHelper.get(parameterCursor, "DocumentID", String.class);
            if (documentID != null) document.setDocumentId(documentID);

            String groupID = IDataHelper.get(parameterCursor, "GroupID", String.class);
            if (groupID != null) document.setGroupId(groupID);

            String conversationID = IDataHelper.get(parameterCursor, "ConversationID", String.class);
            if (conversationID != null) document.setConversationId(conversationID);

            String senderID = IDataHelper.get(parameterCursor, "SenderID", String.class);
            if (senderID != null) {
                IData profile = ProfileCache.getInstance().get(senderID);
                if (profile != null) {
                    document.setSenderId(IDataHelper.get(profile, "ProfileID", String.class));
                }
            }

            String receiverID = IDataHelper.get(parameterCursor, "ReceiverID", String.class);
            if (receiverID != null) {
                IData profile = ProfileCache.getInstance().get(receiverID);
                if (profile != null) {
                    document.setReceiverId(IDataHelper.get(profile, "ProfileID", String.class));
                }
            }

            String doctypeID = IDataHelper.get(parameterCursor, "DoctypeID", String.class);
            if (doctypeID != null && !doctypeID.equals(document.getDocType().getId())) {
                BizDocType type = BizDocTypeHelper.get(doctypeID);
                if (type != null) document.setDocType(type);
            }

            String doctypeName = IDataHelper.get(parameterCursor, "DoctypeName", String.class);
            if (doctypeName != null && !doctypeName.equals(document.getDocType().getName())) {
                BizDocType type = BizDocTypeHelper.getByName(doctypeName);
                if (type != null) document.setDocType(type);
            }
        } finally {
            parameterCursor.destroy();
        }
    }

    /**
     * Persists the given BizDocEnvelope in the Trading Networks database.
     *
     * @param document              The BizDocEnvelope document to be persisted.
     * @param rule                  The RoutingRule used to route the given BizDocEnvelope.
     * @param transportLog          Whether to log the transport info as a content part.
     * @param transportLogPartName  The content part name for the transport log.
     * @param strict                Whether to abort routing if errors exist on the BizDocEnvelope.
     * @throws ServiceException     If routing is aborted or a database error occurs.
     */
    public static void persist(BizDocEnvelope document, RoutingRule rule, boolean transportLog, String transportLogPartName, boolean strict) throws ServiceException {
        if (document != null) {
            BizDocAttributeHelper.normalize(document);

            PreRoutingFlags preRoutingFlags = getPreRoutingFlags(document, rule);
            document.setPersistOption(preRoutingFlags.getPersistOption());

            IData scope = IDataFactory.create();
            IDataCursor cursor = scope.getCursor();
            try {
                IDataHelper.put(scope, "bizdoc", document);
                IDataHelper.put(scope, "flags", preRoutingFlags);
                ServiceHelper.invoke("wm.tn.route:preroute", scope);
            } finally {
                cursor.destroy();
            }

            if (transportLog) BizDocContentHelper.addTransportContentPart(document, transportLogPartName);

            if (strict) {
                ActivityLogEntry[] errors = BizDocEnvelopeHelper.getErrors(document);
                if ((errors != null && errors.length > 0) || "Unknown".equals(document.getDocType().getName())) {
                    ActivityLogHelper.log(EntryType.ERROR, "Unrecoverable", "Processing aborted due to errors encountered", "Processing of document was aborted due to errors encountered while routing in strict mode", document);
                    setStatus(document, "ABORTED", "ABORTED");
                    raiseIfErrors(document);
                }
            }
        }
    }

    /**
     * Returns the PreRoutingFlags for the given BizDocEnvelope and RoutingRule.
     *
     * @param document  The BizDocEnvelope being routed.
     * @param rule      The RoutingRule routing the BizDocEnvelope.
     * @return          The PreRoutingFlags to use when routing.
     */
    private static PreRoutingFlags getPreRoutingFlags(BizDocEnvelope document, RoutingRule rule) {
        PreRoutingFlags preRoutingFlags;
        PreRoutingFlags documentFlags = document.getDocType().getPreRoutingFlags();
        PreRoutingFlags ruleFlags = rule == null ? null : rule.getPreRoutingFlags();

        if (documentFlags == null && ruleFlags == null) {
            preRoutingFlags = new PreRoutingFlags();
        } else if (documentFlags != null && ruleFlags != null) {
            preRoutingFlags = PreRoutingFlags.merge(documentFlags, ruleFlags);
        } else if (documentFlags != null) {
            preRoutingFlags = documentFlags;
        } else {
            preRoutingFlags = ruleFlags;
        }

        return preRoutingFlags;
    }

    /**
     * Returns true if content parts are to be persisted to the database for the given BizDocEnvelope.
     *
     * @param document  The BizDocEnvelope object.
     * @return          True if content parts are to be persisted to the database for the given BizDocEnvelope.
     */
    public static boolean shouldPersistContent(BizDocEnvelope document) {
        return document != null && document.isPersisted() && PreRoutingFlags.isPersistContent(getPersistOption(document));
    }

    /**
     * Returns true if attributes are to be persisted to the database for the given BizDocEnvelope.
     *
     * @param document  The BizDocEnvelope object.
     * @return          True if attributes are to be persisted to the database for the given BizDocEnvelope.
     */
    public static boolean shouldPersistAttributes(BizDocEnvelope document) {
        return document != null && document.isPersisted() && PreRoutingFlags.isPersistAttrib(getPersistOption(document));
    }

    /**
     * Returns true if activity logs are to be persisted to the database for the given BizDocEnvelope.
     *
     * @param document  The BizDocEnvelope object.
     * @return          True if activity logs are to be persisted to the database for the given BizDocEnvelope.
     */
    public static boolean shouldPersistActivityLog(BizDocEnvelope document) {
        return document != null && document.isPersisted() && PreRoutingFlags.isPersistActLog(getPersistOption(document));
    }

    /**
     * Returns the persist option for the given BizDocEnvelope.
     *
     * @param document  The BizDocEnvelope object.
     * @return          The persist option for the given BizDocEnvelope.
     */
    private static String getPersistOption(BizDocEnvelope document) {
        String persistOption = document.getPersistOption();
        if (persistOption == null || persistOption.equals("")) {
            persistOption = document.getDocType().getPreRoutingFlags().getPersistOption();
        }
        return persistOption;
    }

    /**
     * Returns the content schema used for parsing this BizDocEnvelope's content.
     *
     * @param document  The BizDocEnvelope whose content schema is to be returned.
     * @return          The content schema for the given BizDocEnvelope.
     */
    public static String getContentSchema(BizDocEnvelope document) {
        return document == null ? null : BizDocTypeHelper.getContentSchema(document.getDocType());
    }

    /**
     * Returns the content schema type used for parsing this BizDocEnvelope's content.
     *
     * @param document  The BizDocEnvelope whose content schema type is to be returned.
     * @return          The content schema type for the given BizDocEnvelope.
     */
    public static String getContentSchemaType(BizDocEnvelope document) {
        return document == null ? null : BizDocTypeHelper.getContentSchemaType(document.getDocType());
    }

    /**
     * Returns the namespace declarations used for parsing this BizDocEnvelope's content.
     *
     * @param document  The BizDocEnvelope whose namespace declarations are to be returned.
     * @return          The namespace declarations for the given BizDocEnvelope.
     */
    public static IData getNamespaceDeclarations(BizDocEnvelope document) {
        return document == null ? null : BizDocTypeHelper.getNamespaceDeclarations(document.getDocType());
    }

    /**
     * The maximum time a queued task can be in DELIVERING state before it becomes eligible to be restarted.
     */
    private static final long MAXIMUM_TASK_DELIVERING_MILLISECONDS = 10 * 60 * 1000;
    /**
     * The BizDocEnvelope attribute used to defer delivery of a queued task.
     */
    private static final String MESSAGE_EPOCH_ATTRIBUTE_NAME = "Message Epoch";
    /**
     * The datetime patterns used when parsing value of the message epoch attribute.
     */
    private static final String[] MESSAGE_EPOCH_DATETIME_PATTERNS = new String[]{"datetime.jdbc", "datetime", "date", "time"};

    /**
     * Enqueues the given BizDocEnvelope to the given DeliveryQueue.
     *
     * @param document          The document to enqueue.
     * @param queue             The queue to enqueue the document to.
     * @param force             Whether to restart pre-existing task regardless of status.
     * @throws ServiceException If an exception occurs when enqueuing the document.
     */
    public static GuaranteedJob enqueue(BizDocEnvelope document, DeliveryQueue queue, boolean force, String messageSummary, IData context) throws ServiceException {
        GuaranteedJob task = null;
        boolean documentEnqueued = false;

        if (document != null && document.isPersisted() && queue != null) {
            long startTime = System.nanoTime();

            EntryType entryType = EntryType.MESSAGE;
            String messageDetail;

            if (queue.isDraining() || queue.isDisabled()) {
                throw new UnrecoverableException("Document " + document.getInternalId() + " could not be enqueued as " + queue.getQueueType() + " queue " + queue.getQueueName() + " is " + queue.getState());
            } else {
                IData receiver = ProfileCache.getInstance().get(document.getReceiverId());
                if (receiver == null) {
                    throw new UnrecoverableException("Document " + document.getInternalId() + " could not be enqueued as receiver " + document.getReceiverId() + " does not exist");
                } else if (!"Active".equals(IDataHelper.get(receiver, "Status", String.class))) {
                    throw new UnrecoverableException("Document " + document.getInternalId() + " could not be enqueued as receiver " + document.getReceiverId() + " is not active");
                } else {
                    try {
                        if (messageSummary == null) {
                            messageSummary = "Document enqueue";
                        }

                        GuaranteedJob[] existingTasks = GuaranteedJobHelper.list(document);
                        for (GuaranteedJob existingTask : existingTasks) {
                            if (queue.getQueueName().equals(existingTask.getQueueName())) {
                                task = existingTask;
                                if (force || existingTask.getStatusVal() == GuaranteedJob.FAILED || existingTask.getStatusVal() == GuaranteedJob.STOPPED || (existingTask.getStatusVal() == GuaranteedJob.DELIVERING && existingTask.getTimeUpdated() < (System.currentTimeMillis() - MAXIMUM_TASK_DELIVERING_MILLISECONDS))) {
                                    GuaranteedJobHelper.restart(existingTask);
                                    documentEnqueued = true;
                                    break;
                                }
                            }
                        }

                        if (task == null) {
                            task = DeliveryUtils.createQueuedJob(document, queue.getQueueName());

                            if (IDataHelper.getOrDefault(receiver, "Corporation/RoutingOff", Boolean.class, false)) {
                                task.setStatus(DeliveryJob.HELD);
                            }

                            // to support deferring tasks, set TimeCreated on task to Message Epoch document attribute
                            // value if the attribute exists and can be parsed as a datetime and is in the future
                            IData attributes = document.getAttributes();
                            if (attributes != null) {
                                IDataCursor cursor = attributes.getCursor();
                                try {
                                    String messageEpoch = IDataHelper.get(cursor, MESSAGE_EPOCH_ATTRIBUTE_NAME, String.class);
                                    if (messageEpoch != null) {
                                        Calendar epoch = DateTimeHelper.parse(messageEpoch, MESSAGE_EPOCH_DATETIME_PATTERNS);
                                        if (epoch != null) {
                                            long epochMillis = epoch.getTimeInMillis();
                                            if (epochMillis > System.currentTimeMillis()) {
                                                task.setTimeCreated(epochMillis);
                                            }
                                        }
                                    }
                                } catch(Exception ex) {
                                    // ignore exception
                                } finally {
                                    cursor.destroy();
                                }
                            }

                            DeliveryStore.insertJob(task);
                            BizDocStore.queueForDelivery(document);

                            messageSummary = messageSummary + " successful";
                            messageDetail = "Document enqueued for delivery by " + queue.getQueueType() + " queue " + queue.getQueueName() + ": task " + task.getJobId() + " created";
                        } else {
                            if (documentEnqueued) {
                                messageSummary = messageSummary + " successful";
                                messageDetail = "Document enqueued for delivery by " + queue.getQueueType() + " queue " + queue.getQueueName() + ": pre-existing task " + task.getJobId() + " restarted";
                            } else {
                                task = null;
                                entryType = EntryType.WARNING;
                                messageSummary = messageSummary + " ignored";
                                messageDetail = "Document enqueue for delivery by " + queue.getQueueType() + " queue " + queue.getQueueName() + " was ignored: one or more pre-existing tasks are already in-progress or completed";
                            }
                        }

                        ActivityLogHelper.log(entryType, "General", messageSummary, messageDetail, document, ActivityLogHelper.getContext(context, startTime, System.nanoTime()));
                    } catch(Exception ex) {
                        ExceptionHelper.raise(ex);
                    }
                }
            }
        }

        return task;
    }
}
