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

package permafrost.tundra.tn.profile;

import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import permafrost.tundra.tn.cache.PartnerIDCache;
import java.util.SortedSet;

public class InternalID extends ProfileID {
    /**
     * Constructs a new InternalID object.
     *
     * @param identity  The InternalID identity value.
     */
    public InternalID(String identity) {
        if (identity == null) throw new NullPointerException("identity must not be null");
        this.identity = identity;
    }

    /**
     * Returns the ExternalID with the given type associated with this InternalID object.
     *
     * @param type  The ExternalID type.
     * @return      The ExternalID with the given type associated with this InternalID object.
     */
    public SortedSet<ExternalID> getExternalID(Integer type) {
        return PartnerIDCache.InternalToExternal.getInstance().get(this, type);
    }

    /**
     * Returns the ExternalID with the given type name associated with this InternalID object.
     *
     * @param typeName  The ExternalID type name.
     * @return          The ExternalID with the given type name associated with this InternalID object.
     */
    public SortedSet<ExternalID> getExternalID(String typeName) {
        return getExternalID(ExternalID.getType(typeName));
    }

    /**
     * Normalizes this object into a InternalID.
     *
     * @return  This object.
     */
    @Override
    public InternalID intern() {
        return this;
    }

    /**
     * Returns true if this InternalID is associated with an active partner in Trading Networks.
     *
     * @return True if this InternalID is associated with an active partner in Trading Networks.
     */
    @Override
    public boolean exists() {
        return PartnerIDCache.InternalToExternal.getInstance().exists(this);
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

        if (object instanceof InternalID) {
            InternalID other = (InternalID)object;
            result = getIdentity().equals(other.getIdentity());
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
        return getIdentity().hashCode();
    }

    /**
     * Returns an IData representation of this object.
     *
     * @return An IData representation of this object.
     */
    @Override
    public IData getIData() {
        IData document = IDataFactory.create();
        IDataCursor cursor = document.getCursor();

        try {
            cursor.insertAfter("PartnerID", getIdentity());
        } finally {
            cursor.destroy();
        }

        return document;
    }
}
