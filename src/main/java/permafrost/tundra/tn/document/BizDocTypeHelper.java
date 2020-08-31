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

import com.wm.app.tn.db.BizDocTypeStore;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.app.tn.doc.BizDocType;
import com.wm.app.tn.doc.FFDocType;
import com.wm.app.tn.doc.XMLDocType;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.lang.ns.NSName;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.time.DateTimeHelper;
import permafrost.tundra.tn.profile.ProfileHelper;

/**
 * A collection of convenience methods for working with Trading Networks BizDocType objects.
 */
public final class BizDocTypeHelper {
    /**
     * Disallow instantiation of this class.
     */
    private BizDocTypeHelper() {}

    /**
     * Returns the BizDocType object associated with the given the ID.
     *
     * @param id The ID that identifies the BizDocType to be returned.
     * @return   The BizDocType object identified by the given ID.
     */
    public static BizDocType get(String id) {
        return BizDocTypeStore.get(id, true, true);
    }

    /**
     * Returns the BizDocType object associated with the given name.
     *
     * @param name The name of the BizDocType to be returned.
     * @return     The BizDocType object with the given name.
     */
    public static BizDocType getByName(String name) {
        return BizDocTypeStore.getByName(name, true, true);
    }

    /**
     * Returns the content schema used to parse this content for this BizDocType.
     *
     * @param type  The BizDocType whose content schema is to be returned.
     * @return      The content schema for the given BizDocType.
     */
    public static String getContentSchema(BizDocType type) {
        if (type == null) return null;

        String contentSchema = null;

        if (type instanceof XMLDocType) {
            NSName recordBlueprint = ((XMLDocType)type).getRecordBlueprint();
            if (recordBlueprint != null) contentSchema = recordBlueprint.getFullName();
        } else if (type instanceof FFDocType) {
            contentSchema = ((FFDocType)type).getParsingSchema();
        }

        return contentSchema;
    }

    /**
     * Returns the content schema type used to parse this content for this BizDocType.
     *
     * @param type  The BizDocType whose content schema is to be returned.
     * @return      The content schema type for the given BizDocType.
     */
    public static String getContentSchemaType(BizDocType type) {
        if (type == null) return null;

        String contentSchemaType = null;

        if (type instanceof XMLDocType) {
            contentSchemaType = "XML";
        } else if (type instanceof FFDocType) {
            contentSchemaType = "Flat File";
        }

        return contentSchemaType;
    }

    /**
     * Returns the namespace declarations from the given BizDocType.
     *
     * @param type  The BizDocType whose namespace declarations are to be returned.
     * @return      The namespace declarations from the given BizDocType.
     */
    public static IData getNamespaceDeclarations(BizDocType type) {
        IData namespace = null;

        if (type != null) {
            String[][] declarations = (String[][])type.get("nsDecls");
            if (declarations != null) {
                IDataCursor cursor = null;
                try {
                    boolean nsAlreadyExists = false;
                    for (int i = 0; i < declarations.length; i++) {
                        if (declarations[i] != null && declarations[i].length > 1) {
                            String key = declarations[i][0];
                            String value = declarations[i][1];
                            if (key != null && value != null) {
                                if (namespace == null) namespace = IDataFactory.create();
                                if (cursor == null) cursor = namespace.getCursor();
                                if ("ns".equals(key)) nsAlreadyExists = true;
                                if ("prefix0".equals(key) && !nsAlreadyExists) {
                                    // By default Trading Networks uses the `prefix0` prefix to represent the default
                                    // target namespace, whereas Integration Server uses the `ns` prefix.
                                    // We will automatically add the `ns` prefix, unless its already been defined on the
                                    // document type, with the same value as the `prefix0` prefix.
                                    // Importantly, the `ns` prefix needs to precede the `prefix0` prefix in the
                                    // resulting IData document's element insertion order, as the `WmPublic/pub.xml:*`
                                    // services will use the first prefix in the document when parsing / serializing,
                                    // and we want that to be the Integration Server default prefix `ns`.
                                    IDataHelper.put(cursor, "ns", value);
                                }
                                IDataHelper.put(cursor, key, value);
                            }
                        }
                    }
                } finally {
                    if (cursor != null) cursor.destroy();
                }
            }
        }
        return namespace;
    }

    /**
     * Returns the given IData if its already a BizDocType, otherwise converts it to a BizDocType object.
     *
     * @param input The IData object to be converted to a BizDocType object.
     * @return      The BizDocType object that represents the given IData object.
     */
    public static BizDocType normalize(IData input) {
        if (input == null) return null;

        BizDocType output;

        if (input instanceof BizDocType) {
            output = (BizDocType)input;
        } else {
            IDataCursor cursor = input.getCursor();
            try {
                String id = IDataHelper.get(cursor, "TypeID", String.class);
                if (id == null) throw new IllegalArgumentException("TypeID is required");
                output = get(id);
            } finally {
                cursor.destroy();
            }
        }

        return output;
    }

    /**
     * Returns a summary of the given object, suitable for logging.
     *
     * @param object    The object to summarize.
     * @return          The summary of the object.
     */
    public static IData summarize(BizDocType object) {
        IData summary = null;
        if (object != null) {
            summary = IDataFactory.create();
            IDataCursor cursor = summary.getCursor();
            try {
                IDataHelper.put(cursor, "TypeID", object.getId());
                IDataHelper.put(cursor, "TypeName", object.getName());
            } finally {
                cursor.destroy();
            }
        }
        return summary;
    }
}
