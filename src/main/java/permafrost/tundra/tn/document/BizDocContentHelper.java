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

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.db.Datastore;
import com.wm.app.tn.db.SQLWrappers;
import com.wm.app.tn.doc.BizDocContentPart;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.app.tn.doc.BizDocType;
import com.wm.app.tn.doc.FFDocType;
import com.wm.app.tn.doc.XMLDocType;
import permafrost.tundra.io.StreamHelper;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.mime.MIMETypeHelper;

/**
 * A collection of convenience methods for working with Trading Networks BizDocEnvelope content parts.
 */
public final class BizDocContentHelper {
    /**
     * The default content part name for an XML BizDocEnvelope.
     */
    public final static String DEFAULT_PART_NAME_XML = "xmldata";

    /**
     * The default content part name for a Flat File BizDocEnvelope.
     */
    public final static String DEFAULT_PART_NAME_FLAT_FILE = "ffdata";

    /**
     * The default content part name for an unknown BizDocEnvelope.
     */
    public final static String DEFAULT_PART_NAME_UNKNOWN = "bytes";

    /**
     * The SQL statement used to delete a BizDoc content part.
     */
    private static final String DELETE_BIZDOC_CONTENT_SQL = "DELETE FROM BizDocContent WHERE DocID = ? AND PartName = ?";

    /**
     * Disallow instantiation of this class.
     */
    private BizDocContentHelper() {}

    /**
     * Deletes the given BizDocContentPart from the Trading Networks database.
     *
     * @param document          The BizDocEnvelope whose content part is to be deleted.
     * @param partName          The name of the content part to be deleted.
     * @throws ServiceException If a database error occurs.
     */
    public static void removeContentPart(BizDocEnvelope document, String partName) throws ServiceException {
        if (document == null || partName == null) return;

        Connection connection = null;
        PreparedStatement statement = null;

        try {
            connection = Datastore.getConnection();
            statement = connection.prepareStatement(DELETE_BIZDOC_CONTENT_SQL);
            statement.clearParameters();
            SQLWrappers.setCharString(statement, 1, document.getInternalId());
            SQLWrappers.setCharString(statement, 2, partName);
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException ex) {
            connection = Datastore.handleSQLException(connection, ex);
            ExceptionHelper.raise(ex);
        } finally {
            SQLWrappers.close(statement);
            Datastore.releaseConnection(connection);
        }
    }

    /**
     * Deletes the given BizDocContentPart from the Trading Networks database.
     *
     * @param document          The BizDocEnvelope associated with the BizDocContentPart to be deleted.
     * @param contentPart       The BizDocContentPart to be deleted.
     * @throws ServiceException If a database error occurs.
     */
    public static void removeContentPart(BizDocEnvelope document, BizDocContentPart contentPart) throws ServiceException {
        removeContentPart(document, contentPart == null ? null : contentPart.getPartName());
    }

    /**
     * Returns the MIME type associated with the given BizDocContentPart object.
     *
     * @param contentPart       The BizDocContentPart whose MIME type is to be returned.
     * @return                  The MIME type associated with the given BizDocContentPart.
     */
    public static String getContentType(BizDocContentPart contentPart) {
        String contentType = null;

        if (contentPart != null) {
            contentType = contentPart.getMimeType();
        }

        return contentType == null ? MIMETypeHelper.DEFAULT_MIME_TYPE : contentType;
    }

    /**
     * Returns the content associated with the given content part name from the given BizDocEnvelope as an InputStream.
     *
     * @param document          The BizDocEnvelope whose content is to be returned.
     * @param contentPart       The content part name whose content is to be returned.
     * @return                  The BizDocEnvelope content associated with the given part name as an InputStream.
     * @throws ServiceException If an IO error occurs.
     */
    public static InputStream getContent(BizDocEnvelope document, BizDocContentPart contentPart) throws ServiceException {
        if (document == null || contentPart == null) return null;

        InputStream content = null;

        try {
            content = StreamHelper.normalize(contentPart.getContent(document.getInternalId()));
        } catch (IOException ex) {
            ExceptionHelper.raise(ex);
        }

        return content;
    }

    /**
     * Returns the default content associated with the given BizDocEnvelope as an InputStream.
     *
     * @param document          The BizDocEnvelope whose default content is to be returned.
     * @return                  The default BizDocEnvelope content as an InputStream.
     * @throws ServiceException If an IO error occurs.
     */
    public static InputStream getContent(BizDocEnvelope document) throws ServiceException {
        return getContent(document, getContentPart(document));
    }

    /**
     * Returns the BizDocContentPart with the given name associated with the given BizDocEnvelope.
     *
     * @param document The BizDocEnvelope whose content part is to be returned.
     * @param partName The name of the BizDocContentPart to be returned. If null, the default BizDocContentPart is
     *                 returned.
     * @return         The BizDocContentPart with the given name associated with the given BizDocEnvelope.
     */
    public static BizDocContentPart getContentPart(BizDocEnvelope document, String partName) {
        if (document == null) return null;

        BizDocContentPart contentPart = null;

        if (partName == null) {
            BizDocType documentType = document.getDocType();
            if (documentType instanceof XMLDocType) {
                contentPart = document.getContentPart(DEFAULT_PART_NAME_XML);
            } else if (documentType instanceof FFDocType) {
                contentPart = document.getContentPart(DEFAULT_PART_NAME_FLAT_FILE);
            } else {
                contentPart = document.getContentPart(DEFAULT_PART_NAME_XML);

                if (contentPart == null) {
                    contentPart = document.getContentPart(DEFAULT_PART_NAME_FLAT_FILE);
                }

                if (contentPart == null) {
                    contentPart = document.getContentPart(DEFAULT_PART_NAME_UNKNOWN);
                }

                if (contentPart == null) {
                    BizDocContentPart[] contentParts = document.getContentParts();
                    if (contentParts != null && contentParts.length > 0) {
                        // if all else fails, default to the first content part in the list of parts
                        contentPart = contentParts[0];
                    }
                }
            }
        } else {
            contentPart = document.getContentPart(partName);
        }

        return contentPart;
    }

    /**
     * Returns the default BizDocContentPart associated with the given BizDocEnvelope.
     *
     * @param document The BizDocEnvelope whose default content part is to be returned.
     * @return         The default BizDocContentPart associated with the given BizDocEnvelope.
     */
    public static BizDocContentPart getContentPart(BizDocEnvelope document) {
        return getContentPart(document, null);
    }
}
