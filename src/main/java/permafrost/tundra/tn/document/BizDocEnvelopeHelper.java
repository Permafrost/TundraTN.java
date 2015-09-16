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

import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.db.BizDocStore;
import com.wm.app.tn.db.DatastoreException;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
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
     * @param includeContent    Whether to include all content parts with the returned BizDocEnvelope.
     * @return                  The full BizDocEnvelope associated with the given IData document.
     * @throws ServiceException If a database error occurs.
     */
    public static BizDocEnvelope normalize(IData input, boolean includeContent) throws ServiceException {
        if (input == null) return null;

        BizDocEnvelope output = null;

        if (input instanceof BizDocEnvelope) {
            output = (BizDocEnvelope)input;
            if (includeContent && output.getContent() == null) output = get(output.getInternalId(), includeContent);
        } else {
            IDataCursor cursor = input.getCursor();
            String id = IDataUtil.getString(cursor, "InternalID");
            cursor.destroy();

            if (id == null) throw new IllegalArgumentException("InternalID is required");

            output = get(id, includeContent);
        }

        return output;
    }

    /**
     * Returns the BizDocEnvelope associated with the given ID without its associated content parts.
     *
     * @param id                The ID of the BizDoc to be returned.
     * @return                  The BizDoc associated with the given ID.
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

        BizDocEnvelope bizdoc = null;

        try {
            bizdoc = BizDocStore.getDocument(id, includeContent);
        } catch(DatastoreException ex) {
            ExceptionHelper.raise(ex);
        }

        return bizdoc;
    }
}
