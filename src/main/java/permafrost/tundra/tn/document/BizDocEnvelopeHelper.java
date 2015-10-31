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

import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.db.BizDocStore;
import com.wm.app.tn.db.DatastoreException;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.data.IDataUtil;
import permafrost.tundra.lang.ExceptionHelper;

/**
 * A collection of convenience methods for working with Trading Networks BizDocEnvelope objects.
 */
public class BizDocEnvelopeHelper {
    /**
     * Disallow instantiation of this class.
     */
    private BizDocEnvelopeHelper() {}

    /**
     * Returns a full BizDocEnvelope, if given either a subset or full BizDocEnvelope as an IData document.
     *
     * @param input             An IData document which could be a BizDocEnvelope, or could be a subset of a
     *                          BizDocEnvelope that includes an InternalID key.
     * @return                  The full BizDocEnvelope associated with the given IData document.
     * @throws ServiceException If a database error occurs.
     */
    public static BizDocEnvelope normalize(IData input) throws ServiceException {
        return normalize(input, false);
    }

    /**
     * Returns a full BizDocEnvelope, if given either a subset or full BizDocEnvelope as an IData document.
     *
     * @param input             An IData document which could be a BizDocEnvelope, or could be a subset of a
     *                          BizDocEnvelope that includes an InternalID key.
     * @param includeContent    Whether to include all content parts with the returned BizDocEnvelope.
     * @return                  The full BizDocEnvelope associated with the given IData document.
     * @throws ServiceException If a database error occurs.
     */
    public static BizDocEnvelope normalize(IData input, boolean includeContent) throws ServiceException {
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
     * @param id                The ID of the BizDocEnvelope to be returned.
     * @return                  The BizDocEnvelope associated with the given ID.
     * @throws ServiceException If a database exception occurs.
     */
    public static BizDocEnvelope get(String id) throws ServiceException {
        return get(id, false);
    }

    /**
     * Returns the BizDocEnvelope, and optionally its content parts, associated with the given ID.
     *
     * @param id                The ID of the BizDocEnvelope to be returned.
     * @param includeContent    Whether to include all content parts with the returned BizDocEnvelope.
     * @return                  The BizDocEnvelope associated with the given ID.
     * @throws ServiceException If a database exception occurs.
     */
    public static BizDocEnvelope get(String id, boolean includeContent) throws ServiceException {
        if (id == null) return null;

        BizDocEnvelope document = null;

        try {
            document = BizDocStore.getDocument(id, includeContent);
        } catch(DatastoreException ex) {
            ExceptionHelper.raise(ex);
        }

        return document;
    }

    /**
     * Updates the status on the given BizDocEnvelope.
     *
     * @param bizdoc       The BizDocEnvelope to update the status on.
     * @param systemStatus The system status to be set.
     * @param userStatus   The user status to be set.
     * @throws ServiceException If a database error is encountered.
     */
    public static void setStatus(BizDocEnvelope bizdoc, String systemStatus, String userStatus) throws ServiceException {
        if (bizdoc == null) return;
        BizDocStore.changeStatus(bizdoc, systemStatus, userStatus);
    }

    /**
     * The implementation service for BizDocEnvelope logging.
     */
    private static final com.wm.lang.ns.NSName LOG_SERVICE = com.wm.lang.ns.NSName.create("tundra.tn:log");

    /**
     * Adds an activity log statement to the given BizDocEnvelope.
     * TODO: convert this to a pure java service, rather than an invoke of a flow service.
     *
     * @param bizdoc         The BizDocEnvelope to add the activity log statement to.
     * @param messageType    The type of message to be logged.
     * @param messageClass   The class of the message to be logged.
     * @param messageSummary The summary of the message to be logged.
     * @param messageDetails The detail of the message to be logged.
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
}
