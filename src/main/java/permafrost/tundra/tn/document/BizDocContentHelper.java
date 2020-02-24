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
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.db.Datastore;
import com.wm.app.tn.db.SQLWrappers;
import com.wm.app.tn.doc.BizDocContentPart;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.app.tn.doc.BizDocType;
import com.wm.app.tn.doc.FFDocType;
import com.wm.app.tn.doc.XMLDocType;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.lang.ns.NSService;
import permafrost.tundra.collection.CollectionHelper;
import permafrost.tundra.content.ContentParser;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.data.IDataYAMLParser;
import permafrost.tundra.io.InputStreamHelper;
import permafrost.tundra.lang.CharsetHelper;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.mime.MIMETypeHelper;
import permafrost.tundra.server.InvokeStateHelper;
import permafrost.tundra.server.ServiceHelper;
import permafrost.tundra.time.DateTimeHelper;
import permafrost.tundra.time.DurationHelper;
import permafrost.tundra.time.DurationPattern;
import permafrost.tundra.tn.log.ActivityLogHelper;
import permafrost.tundra.tn.log.EntryType;
import javax.activation.MimeType;

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
     * Adds a new BizDocContentPart to the given BizDocEnvelope document.
     *
     * @param document          The document against which to add the new content part.
     * @param partName          The name of the new content part.
     * @param contentType       The MIME content type the new content part uses.
     * @param charset           The character set the new content part uses, if any.
     * @param content           The data content for the new content part.
     * @param overwrite         Whether to overwrite an existing part with the same name.
     * @throws ServiceException If an error occurs.
     */
    public static void addContentPart(BizDocEnvelope document, String partName, String contentType, Charset charset, InputStream content, boolean overwrite) throws ServiceException {
        if (overwrite) {
            BizDocContentPart contentPart = document.getContentPart(partName);
            if (contentPart != null) {
                removeContentPart(document, partName);
            }
        }

        MimeType mimeType = MIMETypeHelper.normalize(MIMETypeHelper.of(contentType));
        final String charsetParameterName = "charset";
        if (charset != null) {
            mimeType.setParameter(charsetParameterName, charset.displayName());
        } else if (mimeType.getParameter(charsetParameterName) == null && MIMETypeHelper.isText(mimeType)) {
            mimeType.setParameter(charsetParameterName, CharsetHelper.DEFAULT_CHARSET_NAME);
        }

        IData pipeline = IDataFactory.create();
        IDataCursor cursor = pipeline.getCursor();
        try {
            IDataHelper.put(cursor, "bizdoc", document);
            IDataHelper.put(cursor, "partName", partName);
            IDataHelper.put(cursor, "partStream", content);
            IDataHelper.put(cursor, "mimeType", mimeType.toString());

            Service.doInvoke("wm.tn.doc", "addContentPart", pipeline);
        } catch(Exception ex) {
            ExceptionHelper.raise(ex);
        } finally {
            cursor.destroy();
        }
    }

    /**
     * Deletes the given BizDocContentPart from the Trading Networks database.
     *
     * @param document          The BizDocEnvelope whose content part is to be deleted.
     * @param partName          The name of the content part to be deleted.
     * @throws ServiceException If a database error occurs.
     */
    public static void removeContentPart(BizDocEnvelope document, String partName) throws ServiceException {
        if (document == null || partName == null) return;

        if (BizDocEnvelopeHelper.shouldPersistContent(document)) {
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

        BizDocContentPart[] originalContentParts = document.getContentParts();
        if (originalContentParts != null && originalContentParts.length > 0) {
            List<BizDocContentPart> newContentParts = new ArrayList<BizDocContentPart>(originalContentParts.length);
            for (BizDocContentPart contentPart : originalContentParts) {
                if (!partName.equals(contentPart.getPartName())) {
                    newContentParts.add(contentPart);
                }
            }
            document.setContentParts(newContentParts.toArray(new BizDocContentPart[0]));
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
     * Logs the current transport information against the given BizDocEnvelope document as a new content part.
     *
     * @param document  The BizDocEnvelope to log the current transport against.
     */
    public static void addTransportContentPart(BizDocEnvelope document) throws ServiceException {
        addTransportContentPart(document, null);
    }

    /**
     * Logs the current transport information against the given BizDocEnvelope document as a new content part.
     *
     * @param document  The BizDocEnvelope to log the current transport against.
     * @param document  The BizDocEnvelope to log the current transport against.
     */
    public static void addTransportContentPart(BizDocEnvelope document, String contentPartName) throws ServiceException {
        if (BizDocEnvelopeHelper.shouldPersistContent(document)) {
            String currentDateTime = DateTimeHelper.now("datetime");

            List<NSService> callStack = ServiceHelper.getCallStack();
            if (contentPartName == null) {
                contentPartName = MessageFormat.format("tundra_tn_receive_transport_{0}.yaml", DateTimeHelper.format(currentDateTime, "datetime", "yyyyMMddHHmmssSSSZ"));
            }

            IData transport = InvokeStateHelper.currentRedactedTransport();

            if (transport != null) {
                IData contentPart = IDataFactory.create();
                IDataCursor contentPartCursor = contentPart.getCursor();
                try {
                    IDataHelper.put(contentPartCursor, "datetime", currentDateTime);
                    IDataHelper.put(contentPartCursor, "transport", transport);
                    if (callStack.size() > 0) IDataHelper.put(contentPartCursor, "callstack", CollectionHelper.stringify(callStack));
                } finally {
                    contentPartCursor.destroy();
                }

                IDataYAMLParser parser = new IDataYAMLParser();
                try {
                    InputStream content = parser.emit(contentPart, CharsetHelper.DEFAULT_CHARSET, InputStream.class);
                    BizDocContentHelper.addContentPart(document, contentPartName, "text/yaml", CharsetHelper.DEFAULT_CHARSET, content, true);
                } catch(IOException ex) {
                    ExceptionHelper.raise(ex);
                }
            }
        }
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

        return contentType == null ? MIMETypeHelper.DEFAULT_MIME_TYPE_STRING : contentType;
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
            Object partContent = contentPart.getContent(document.getInternalId());
            if (partContent == null && contentPart.getLength() == 0 && !contentPart.isLargePart()) {
                partContent = new byte[0];
            }
            content = InputStreamHelper.normalize(partContent);
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

        BizDocContentPart contentPart;

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

    /**
     * Parses the given BizDocEnvelope's content with the given part name.
     *
     * @param document          The BizDocEnvelope whose content is to be parsed.
     * @param partName          The optional name of the content part to be parsed.
     * @param validate          Whether the content should be validated against the schema.
     * @param log               Whether to log the duration of the parse against the BizDocEnvelope.
     * @param pipeline          Optional pipeline containing other settings for the parser.
     * @return                  The parsed content.
     * @throws ServiceException If an error occurs retrieving or parsing the content.
     */
    public static IData parse(BizDocEnvelope document, String partName, boolean validate, boolean log, IData pipeline) throws ServiceException {
        long startTime = System.nanoTime();

        IData parsedContent = null;

        if (document != null) {
            try {
                String contentSchema = BizDocEnvelopeHelper.getContentSchema(document);
                BizDocContentPart contentPart = getContentPart(document, partName);
                if (contentPart != null) {
                    InputStream content = BizDocContentHelper.getContent(document, contentPart);
                    MimeType contentType = MIMETypeHelper.of(contentPart.getMimeType());
                    Charset contentEncoding = null;
                    if (contentType != null) {
                        contentEncoding = CharsetHelper.of(contentType.getParameter("charset"));
                    }
                    IData contentNamespace = BizDocEnvelopeHelper.getNamespaceDeclarations(document);

                    ContentParser parser = new ContentParser(contentType, contentEncoding, contentSchema, contentNamespace, validate, pipeline);

                    parsedContent = parser.parse(content);

                    if (log) {
                        String message;
                        if (contentSchema == null) {
                            message = "Content parse by service tundra.tn.document:parse was successful";
                        } else {
                            message = "Content parse by service tundra.tn.document:parse using schema " + contentSchema + " was successful";
                        }
                        IData context = IDataFactory.create();
                        IDataHelper.put(context, "Duration", DurationHelper.format((System.nanoTime() - startTime) / 1000000000.0, DurationPattern.XML_NANOSECONDS));
                        ActivityLogHelper.log(EntryType.MESSAGE, "General", "Content parse successful", message, document, context);
                    }
                }
            } catch (IOException ex) {
                ExceptionHelper.raise(ex);
            }
        }

        return parsedContent;
    }

    /**
     * Returns whether the given BizDocEnvelope contains a BizDocContentPart with the given part name.
     *
     * @param document  The BizDocEnvelope to check the BizDocContentPart existence on.
     * @param partName  The part name of the BizDocContentPart to check existence of.
     * @return          True if there exists a BizDocContentPart with the given part name on the given document.
     */
    public static boolean exists(BizDocEnvelope document, String partName) {
        return document != null && document.getContentPart(partName) != null;
    }
}
