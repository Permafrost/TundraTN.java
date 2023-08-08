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

import com.wm.app.tn.profile.LookupStoreException;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import permafrost.tundra.tn.cache.IDTypeCache;
import permafrost.tundra.tn.cache.PartnerIDCache;

/**
 * Represents a Trading Partner profile External ID.
 */
public class ExternalID extends ProfileID {
    /**
     * The ExternalID type.
     */
    protected Integer type;
    /**
     * The optional sequence used for ordering multiple ExternalIDs with the same type for the same partner profile.
     */
    protected Integer sequence;

    /**
     * Construct a new ExternalID object.
     *
     * @param identity  The ExternalID identity.
     * @param type      The ExternalID type.
     */
    public ExternalID(String identity, Integer type) {
        this(identity, type, null);
    }

    /**
     * Construct a new ExternalID object.
     *
     * @param identity  The ExternalID identity.
     * @param type      The ExternalID type.
     * @param sequence  The ExternalID sequence.
     */
    public ExternalID(String identity, Integer type, Integer sequence) {
        if (identity == null) throw new NullPointerException("identity must not be null");
        if (type == null) throw new NullPointerException("type must not be null");

        this.type = type;
        this.identity = identity;
        this.sequence = sequence == null ? 0 : sequence;
    }

    /**
     * Construct a new ExternalID object.
     *
     * @param identity  The ExternalID identity.
     * @param typeName  The ExternalID type name.
     */
    public ExternalID(String identity, String typeName) {
        this(identity, typeName, null);
    }

    /**
     * Construct a new ExternalID object.
     *
     * @param identity  The ExternalID identity.
     * @param typeName  The ExternalID type name.
     * @param sequence  The ExternalID sequence.
     */
    public ExternalID(String identity, String typeName, Integer sequence) {
        if (identity == null) throw new NullPointerException("identity must not be null");
        if (typeName == null) throw new NullPointerException("typeName must not be null");

        this.type = getType(typeName);
        if (this.type == null) {
            throw new IllegalArgumentException(new LookupStoreException("Trading Networks partner profile external ID type does not exist: " + typeName));
        }
        this.identity = identity;
        this.sequence = sequence == null ? 0 : sequence;
    }

    /**
     * Normalizes this ExternalID into a InternalID.
     *
     * @return  A InternalID that represents the profile this ExternalID is associated with.
     */
    @Override
    public InternalID intern() {
        return PartnerIDCache.ExternalToInternal.getInstance().get(this);
    }

    /**
     * Returns true if this ExternalID is associated with an active partner in Trading Networks.
     *
     * @return True if this ExternalID is associated with an active partner in Trading Networks.
     */
    @Override
    public boolean exists() {
        return PartnerIDCache.ExternalToInternal.getInstance().exists(this);
    }

    /**
     * Returns the ExternalID type.
     *
     * @return The ExternalID type.
     */
    public Integer getType() {
        return type;
    }

    /**
     * Returns the ExternalID type name.
     *
     * @return The ExternalID type name.
     */
    public String getTypeName() {
        return getTypeName(this.type);
    }

    /**
     * Returns the ExternalID sequence.
     *
     * @return The ExternalID sequence.
     */
    public Integer getSequence() {
        return sequence;
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

        if (object instanceof ExternalID) {
            ExternalID other = (ExternalID)object;
            result = getIdentity().equals(other.getIdentity()) && getType().equals(other.getType());
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
        return getIdentity().hashCode() ^ getType().hashCode();
    }

    /**
     * Returns the ExternalID type name associated with the given type.
     *
     * @param type  The ExternalID type.
     * @return      The type name associated with the given type.
     */
    public static String getTypeName(Integer type) {
        return IDTypeCache.TypeToDescription.getInstance().get(type);
    }

    /**
     * Returns the ExternalID type associated with the given type name.
     *
     * @param typeName  The ExternalID type name.
     * @return          The type associated with the given type name.
     */
    public static Integer getType(String typeName) {
        return IDTypeCache.DescriptionToType.getInstance().get(typeName);
    }

    /**
     * Compares this ProfileID to another ProfileID for sorting.
     *
     * @param other The object to be compared.
     * @return      The sort order for this object.
     */
    @Override
    public int compareTo(ProfileID other) {
        int comparison;

        if (other instanceof ExternalID) {
            ExternalID otherExternalID = (ExternalID)other;
            comparison = getType().compareTo(otherExternalID.getType());
            if (comparison == 0) {
                comparison = getSequence().compareTo(otherExternalID.getSequence());
                if (comparison == 0) {
                    comparison = super.compareTo(other);
                }
            }
        } else {
            comparison = 1;
        }
        return comparison;
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
            cursor.insertAfter(getTypeName(), getIdentity());
        } finally {
            cursor.destroy();
        }

        return document;
    }

    /**
     * Returns a string representation of this object.
     *
     * @return A string representation of this object.
     */
    @Override
    public String toString() {
        return getTypeName() + "=" + identity;
    }
}
