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

import com.wm.data.IData;
import com.wm.util.coder.IDataCodable;

/**
 * Represents either an internal or external Trading Networks partner profile ID.
 */
public abstract class ProfileID implements Comparable<ProfileID>, IDataCodable {
    /**
     * The identity value.
     */
    protected String identity;

    /**
     * Returns this object's identity value.
     *
     * @return This object's identity value.
     */
    public String getIdentity() {
        return identity;
    }

    /**
     * Normalizes this object returning the canonical InternalID representation.
     *
     * @return The InternalID representation.
     */
    abstract public ProfileID intern();

    /**
     * Returns true if this ProfileID is associated with an active partner in Trading Networks.
     *
     * @return True if this ProfileID is associated with an active partner in Trading Networks.
     */
    abstract public boolean exists();

    /**
     * Compares this ProfileID to another ProfileID for sorting.
     *
     * @param other The object to be compared.
     * @return      The sort order for this object.
     */
    @Override
    public int compareTo(ProfileID other) {
        return getIdentity().compareTo(other.getIdentity());
    }

    /**
     * Not implemented.
     *
     * @param document Do not use.
     */
    @Override
    public void setIData(IData document) {
        throw new UnsupportedOperationException("setIData(IData) not implemented");
    }

    /**
     * Returns a string representation of this object.
     *
     * @return A string representation of this object.
     */
    @Override
    public String toString() {
        return identity;
    }
}
