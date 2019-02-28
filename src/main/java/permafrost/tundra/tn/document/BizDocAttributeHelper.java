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

package permafrost.tundra.tn.document;

import com.wm.app.tn.db.BizDocAttributeStore;
import com.wm.app.tn.doc.BizDocAttribute;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import java.util.Enumeration;
import java.util.Set;
import java.util.TreeSet;

/**
 * A collection of convenience methods for working with Trading Networks BizDocAttribute objects.
 */
public final class BizDocAttributeHelper {
    /**
     * Disallow instantiation of this class.
     */
    private BizDocAttributeHelper() {}

    /**
     * Returns the BizDocAttribute with the given name.
     *
     * @param name The name of the BizDocAttribute.
     * @return     The BizDocAttribute with the given name, or null if no such attribute exists.
     */
    public static BizDocAttribute getByName(String name) {
        return BizDocAttributeStore.getByName(name, false);
    }

    /**
     * Returns the BizDocAttribute with the given identity.
     *
     * @param id The identity of the BizDocAttribute.
     * @return   The BizDocAttribute with the given identity, or null if no such attribute exists.
     */
    public static BizDocAttribute get(String id) {
        return BizDocAttributeStore.get(id, false);
    }

    /**
     * Returns a new IData document that contains only the tuple's whose keys identify by name an existing and active
     * BizDocAttribute object.
     *
     * @param input The IData document to be normalized.
     * @return      The normalized IData document.
     */
    public static IData sanitize(IData input) {
        if (input == null) return null;

        IData output = IDataFactory.create();

        IDataCursor inputCursor = input.getCursor();
        IDataCursor outputCursor = output.getCursor();

        try {
            Set<String> names = getNames();

            while(inputCursor.next()) {
                String key = inputCursor.getKey();
                Object value = inputCursor.getValue();
                if (names.contains(key)) {
                    outputCursor.insertAfter(key, value);
                }
            }
        } finally {
            inputCursor.destroy();
            outputCursor.destroy();
        }

        return output;
    }

    /**
     * Returns the set of active BizDocAttribute names.
     * @return the set of active BizDocAttribute names.
     */
    public static Set<String> getNames() {
        Set<String> names = new TreeSet<String>();

        Enumeration enumeration = BizDocAttributeStore.list(false);
        while(enumeration.hasMoreElements()) {
            Object object = enumeration.nextElement();
            if (object instanceof BizDocAttribute) {
                BizDocAttribute attribute = (BizDocAttribute)object;
                names.add(attribute.getName());
            }
        }

        return names;
    }
}
