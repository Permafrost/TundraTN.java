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

import com.wm.app.b2b.server.ns.Namespace;
import com.wm.app.tn.db.BizDocTypeStore;
import com.wm.app.tn.doc.BizDocType;
import com.wm.app.tn.doc.FFDocType;
import com.wm.app.tn.doc.XMLDocType;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.lang.ns.NSField;
import com.wm.lang.ns.NSName;
import com.wm.lang.ns.NSNode;
import com.wm.lang.ns.NSRecord;
import permafrost.tundra.data.IDataHelper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A collection of convenience methods for working with Trading Networks BizDocType objects.
 */
public final class BizDocTypeHelper {
    /**
     * The default namespace prefix used by Trading Networks.
     */
    public static final String DEFAULT_NAMESPACE_PREFIX_TN = "prefix0";
    /**
     * The default namespace prefix used by Integration Server.
     */
    public static final String DEFAULT_NAMESPACE_PREFIX_IS = "ns";
    /**
     * Regular expression pattern used for matching against a namespace prefix in an XML tag name.
     */
    private static final Pattern NAMESPACE_PREFIX = Pattern.compile("((.+):)?(.+)");
    
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
                    String defaultNamespacePrefixIS = getDefaultNamespacePrefix(type);
                    boolean defaultNamespacePrefixISAlreadyExists = false;
                    for (String[] declaration : declarations) {
                        if (declaration != null && declaration.length > 1) {
                            String key = declaration[0];
                            String value = declaration[1];
                            if (key != null && value != null) {
                                if (namespace == null) namespace = IDataFactory.create();
                                if (cursor == null) cursor = namespace.getCursor();
                                if (defaultNamespacePrefixIS.equals(key)) defaultNamespacePrefixISAlreadyExists = true;
                                if (DEFAULT_NAMESPACE_PREFIX_TN.equals(key) && !defaultNamespacePrefixISAlreadyExists) {
                                    // Trading Networks uses the `prefix0` prefix to represent the default target
                                    // namespace. When importing an XML schema, Integration Server uses the `ns` prefix
                                    // by default, however the prefix can be customised at import time to an arbitrary
                                    // prefix. Therefore, we will try to determine the correct prefix to use by
                                    // inspecting the first element on the record blueprint on the TN document type, and
                                    // using the prefix specified there if applicable.
                                    // Then we automatically add this Integration Server prefix, unless its already
                                    // been defined on the document type, with the same value as the `prefix0` prefix.
                                    // Importantly, the Integration Server prefix needs to precede the `prefix0` prefix
                                    // in the resulting IData document's element insertion order, as the
                                    // `WmPublic/pub.xml:*` services will use the first prefix in the document when
                                    // parsing / serializing, and we want that to be the Integration Server prefix.
                                    IDataHelper.put(cursor, defaultNamespacePrefixIS, value);
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
     * Returns the prefix to use for the default namespace by checking the root tag on the record blueprint on the
     * given document type, if any.
     *
     * @param docType   The document type.
     * @return          The prefix to use for the default namespace.
     */
    private static String getDefaultNamespacePrefix(BizDocType docType) {
        return getDefaultNamespacePrefix(docType instanceof XMLDocType ? ((XMLDocType)docType).getRecordBlueprint() : null);
    }

    /**
     * Returns the prefix to use for the default namespace by checking the root tag on the given node.
     *
     * @param nodeName  The document reference representing the XML structure.
     * @return          The prefix to use for the default namespace.
     */
    private static String getDefaultNamespacePrefix(NSName nodeName) {
        return getDefaultNamespacePrefix(nodeName == null ? null : Namespace.current().getNode(nodeName));
    }

    /**
     * Returns the prefix to use for the default namespace by checking the root tag on the given node.
     *
     * @param node      The document reference representing the XML structure.
     * @return          The prefix to use for the default namespace.
     */
    private static String getDefaultNamespacePrefix(NSNode node) {
        return getDefaultNamespacePrefix(node instanceof NSRecord ? (NSRecord)node : null);
    }

    /**
     * Returns the prefix to use for the default namespace by checking the root tag on the given record.
     *
     * @param record    The document reference representing the XML structure.
     * @return          The prefix to use for the default namespace.
     */
    private static String getDefaultNamespacePrefix(NSRecord record) {
        String prefix = DEFAULT_NAMESPACE_PREFIX_IS;
        if (record != null) {
            NSField[] fields = record.getFields();
            if (fields != null && fields.length > 0) {
                NSField field = fields[0]; // first field should be root tag
                if (field != null) {
                    String fieldName = field.getNCName();
                    Matcher matcher = NAMESPACE_PREFIX.matcher(fieldName);
                    if (matcher.matches()) {
                        prefix = matcher.group(2);
                        if (prefix == null) {
                            prefix = ""; // use an empty string as prefix when no prefix is present on root tag
                        }
                    }
                }
            }
        }
        return prefix;
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
