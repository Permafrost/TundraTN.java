/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Lachlan Dowding
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

package permafrost.tundra.tn.util;

import com.wm.app.tn.util.TNFixedData;
import com.wm.data.IData;
import permafrost.tundra.data.IDataHelper;

/**
 * Convenience methods for working with TNFixedData objects.
 */
public class TNFixedDataHelper {
    /**
     * Disallow instantiation of this class.
     */
    private TNFixedDataHelper() {}

    /**
     * Duplicates the given TNFixedData object field for field.
     *
     * @param data  The data to be duplicated.
     * @param <T>   The class of the data.
     * @return      A field for field duplicate of the given data.
     */
    @SuppressWarnings("unchecked")
    public static <T extends TNFixedData> T duplicate(T data) {
        if (data == null) return null;

        T dup = (T)data.clone();
        for(int i = 0; i < data.dataSize(); i++) {
            Object value = data.get(i);
            if (value instanceof TNFixedData) {
                value = duplicate((TNFixedData) value);
            } else if (value instanceof IData[]) {
                value = IDataHelper.duplicate((IData[])value, true);
            } else if (value instanceof IData) {
                value = IDataHelper.duplicate((IData)value, true);
            }
            dup.set(i, value);
        }

        return dup;
    }
}
