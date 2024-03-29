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

import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.db.BizDocAttributeStore;
import com.wm.app.tn.db.BizDocStore;
import com.wm.app.tn.doc.BizDocAttribute;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.data.transform.Transformer;
import permafrost.tundra.data.transform.string.Trimmer;
import permafrost.tundra.flow.variable.SubstitutionHelper;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.lang.ObjectHelper;
import permafrost.tundra.server.SystemHelper;
import permafrost.tundra.time.DateTimeHelper;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

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
     * Returns a list of BizDocAttribute objects defined sorted by attribute name.
     *
     * @return A list of BizDocAttribute objects defined sorted by attribute name.
     */
    public static BizDocAttribute[] list() {
        List<BizDocAttribute> list = new ArrayList<BizDocAttribute>();

        Enumeration enumeration = BizDocAttributeStore.list(false);
        while(enumeration.hasMoreElements()) {
            BizDocAttribute attribute = (BizDocAttribute)enumeration.nextElement();
            list.add(attribute);
        }

        // sort by attribute name
        Collections.sort(list, new BizDocAttributeNameComparator());

        return list.toArray(new BizDocAttribute[0]);
    }

    /**
     * BizDocType Comparator which compares based on name.
     */
    public static class BizDocAttributeNameComparator implements Comparator<BizDocAttribute> {
        /**
         * Compares two document attributes using the name.
         *
         * @param attribute         The first object to be compared.
         * @param otherAttribute    The second object to be compared.
         * @return                  The result of the comparison.
         */
        @Override
        public int compare(BizDocAttribute attribute, BizDocAttribute otherAttribute) {
            return attribute.getName().compareTo(otherAttribute.getName());
        }
    }

    /**
     * The regular expression pattern to use to identify if $document was references in a variable substitution string.
     */
    private static final Pattern DOCUMENT_REFERENCE_PATTERN = Pattern.compile("%(\\$document[^%]*)%");

    /**
     * Performs variable substitution on the given attributes and then merges them into the given BizDocEnvelope.
     *
     * @param bizdoc        The BizDocEnvelope to merge the given attributes into.
     * @param attributes    The attributes to be merged.
     * @param scope         The scope against which variable substitution is resolved.
     * @param substitute    Whether to perform variable substitution on the attribute values.
     */
    public static void merge(BizDocEnvelope bizdoc, IData attributes, IData scope, boolean substitute) throws ServiceException {
        if (bizdoc != null && attributes != null) {
            attributes = sanitize(attributes);
            if (!IDataHelper.isEmpty(attributes)) {
                if (substitute) {
                    attributes = SubstitutionHelper.substitute(attributes, null, true, false, null, getScope(bizdoc, attributes, scope));
                }

                attributes = Transformer.transform(attributes, new Trimmer(true));

                replace(bizdoc, IDataHelper.merge(bizdoc.getAttributes(), attributes));

                if (BizDocEnvelopeHelper.shouldPersistAttributes(bizdoc)) BizDocStore.updateAttributes(bizdoc);
            }
        }
    }

    /**
     * Returns the scope required for merging attributes into a BizDocEnvelope.
     *
     * @param bizdoc            The BizDocEnvelope to merge the given attributes into.
     * @param attributes        The attributes to be merged.
     * @param scope             The scope against which variable substitution is resolved.
     * @return                  The modified scope to use for variable substitution.
     * @throws ServiceException If an error occurs when parsing the BizDocEnvelope content.
     */
    private static IData getScope(BizDocEnvelope bizdoc, IData attributes, IData scope) throws ServiceException {
        if (scope == null) {
            scope = IDataFactory.create();
        } else {
            scope = IDataHelper.duplicate(scope);
        }

        if (bizdoc != null && attributes != null) {
            IDataCursor cursor = attributes.getCursor();
            try {
                while (cursor.next()) {
                    Object value = cursor.getValue();
                    if (value instanceof String) {
                        if (DOCUMENT_REFERENCE_PATTERN.matcher((String)value).find()) {
                            IDataHelper.put(scope, "$document", BizDocContentHelper.parse(bizdoc, null, false, false, null));
                            break;
                        }
                    }
                }
                try {
                    IDataHelper.put(scope, "$system", SystemHelper.reflect());
                } catch(ServiceException ex) {
                    // ignore exception
                }
            } finally {
                cursor.destroy();
            }
        }

        return scope;
    }

    /**
     * The index of the attributes element in a BizDocEnvelope.
     */
    private static final int BIZDOC_ATTRIBUTES_INDEX = 11;

    /**
     * Normalizes the attributes in the given BizDocEnvelope, by removing any
     * attributes that do not exist or are not active, and by reformatting any
     * datetime or number attributes to be the expected format for Trading
     * Networks.
     *
     * @param bizdoc    The bizdoc whose attributes are to be normalized.
     */
    public static void normalize(BizDocEnvelope bizdoc) {
        if (bizdoc != null) {
            replace(bizdoc, sanitize(bizdoc.getAttributes()));
        }
    }

    /**
     * Replaces all attributes in the given BizDocEnvelope with the given attributes.
     *
     * @param bizdoc        The BizDocEnvelope to replace the attributes on.
     * @param attributes    The replacement attributes.
     */
    public static void replace(BizDocEnvelope bizdoc, IData attributes) {
        if (bizdoc != null && attributes != null) {
            bizdoc.set(BIZDOC_ATTRIBUTES_INDEX, IDataFactory.create());
            set(bizdoc, attributes);
        }
    }

    /**
     * Updates the given BizDocEnvelope with the given attributes.
     *
     * @param bizdoc        The BizDocEnvelope to set the given attributes on.
     * @param attributes    The attributes to set.
     */
    public static void set(BizDocEnvelope bizdoc, IData attributes) {
        if (bizdoc != null && attributes != null) {
            IDataCursor cursor = attributes.getCursor();
            try {
                while(cursor.next()) {
                    set(bizdoc, cursor.getKey(), cursor.getValue());
                }
            } finally {
                cursor.destroy();
            }
        }
    }

    /**
     * The datetime patterns used to attempt to parse strings when setting the value of attributes of type DATETIME
     * or DATETIME LIST.
     */
    private static final String[] DEFAULT_DATETIME_PATTERNS = new String[] { "datetime.jdbc", "milliseconds", "datetime", "date", "time", "time.jdbc" };

    /**
     * Updates the given BizDocEnvelope with the given attribute key and value.
     *
     * @param bizdoc    The BizDocEnvelope to set the given attribute on.
     * @param key       The attribute key to set.
     * @param value     The attribute value to set.
     */
    public static void set(BizDocEnvelope bizdoc, String key, Object value) {
        if (bizdoc != null && key != null) {
            BizDocAttribute attribute = BizDocAttributeStore.getByName(key, false);
            if (attribute != null) {
                if (attribute.isDate()) {
                    if (value instanceof String) {
                        value = parseDateTime((String)value);
                    }
                } else if (attribute.isDateList()) {
                    if (value instanceof String) {
                        value = new String[]{ (String)value };
                    }
                    if (value instanceof String[]) {
                        value = parseDateTime((String[])value);
                    }
                } else if (attribute.isNumeric()) {
                    if (value instanceof String) {
                        value = parseNumberAttribute((String)value);
                    }
                } else if (attribute.isNumberList()) {
                    if (value instanceof String) {
                        value = new String[]{ (String)value };
                    }
                    if (value instanceof String[]) {
                        String[] inputArray = (String[])value;
                        Double[] outputArray = new Double[inputArray.length];
                        for (int i = 0; i < inputArray.length; i++) {
                            if (inputArray[i] != null) {
                                outputArray[i] = parseNumberAttribute(inputArray[i]);
                            }
                        }
                        value = outputArray;
                    }
                }
            }

            if (value instanceof String) {
                bizdoc.setStringValue(key, (String)value);
            } else if (value instanceof Double) {
                bizdoc.setNumberValue(key, (Double)value);
            } else if (value instanceof Number) {
                bizdoc.setNumberValue(key, ((Number)value).doubleValue());
            } else if (value instanceof Timestamp) {
                bizdoc.setDateValue(key, (Timestamp)value);
            } else if (value instanceof Date) {
                bizdoc.setDateValue(key, new Timestamp(((Date)value).getTime()));
            } else if (value instanceof Calendar) {
                bizdoc.setDateValue(key, new Timestamp(((Calendar)value).getTimeInMillis()));
            } else if (value instanceof String[]) {
                bizdoc.setStringListValue(key, (String[])value);
            } else if (value instanceof Double[]) {
                bizdoc.setNumberListValue(key, (Double[])value);
            } else if (value instanceof Number[]) {
                Number[] input = (Number[])value;
                Double[] output = new Double[input.length];
                for (int i = 0; i < input.length; i++) {
                    output[i] = input[i] == null ? null : input[i].doubleValue();
                }
                bizdoc.setNumberListValue(key, output);
            } else if (value instanceof Timestamp[]) {
                bizdoc.setDateListValue(key, (Timestamp[])value);
            } else if (value instanceof Date[]) {
                Date[] input = (Date[])value;
                Timestamp[] output = new Timestamp[input.length];
                for (int i = 0; i < input.length; i++) {
                    output[i] = input[i] == null ? null : new Timestamp(input[i].getTime());
                }
                bizdoc.setDateListValue(key, output);
            } else if (value instanceof Calendar[]) {
                Calendar[] input = (Calendar[])value;
                Timestamp[] output = new Timestamp[input.length];
                for (int i = 0; i < input.length; i++) {
                    output[i] = input[i] == null ? null : new Timestamp(input[i].getTimeInMillis());
                }
                bizdoc.setDateListValue(key, output);
            } else {
                bizdoc.setStringValue(key, ObjectHelper.convert(value, String.class));
            }
        }
    }

    /**
     * Parses the given datetime string returning java.sql.Timestamp object.
     *
     * @param inputValue    The datetime string to parse.
     * @return              The datetime string parsed as a java.sql.Timestamp object.
     */
    private static Timestamp parseDateTime(String inputValue) {
        Timestamp outputValue;
        if (inputValue == null) {
            outputValue = null;
        } else {
            outputValue = new Timestamp(DateTimeHelper.parse(inputValue, DEFAULT_DATETIME_PATTERNS).getTimeInMillis());
        }
        return outputValue;
    }

    /**
     * Parses the given datetime strings returning java.sql.Timestamp objects.
     *
     * @param inputValues   The datetime strings to parse.
     * @return              The datetime strings parsed as a java.sql.Timestamp objects.
     */
    private static Timestamp[] parseDateTime(String[] inputValues) {
        Timestamp[] outputValues;
        if (inputValues == null) {
            outputValues = null;
        } else {
            outputValues = new Timestamp[inputValues.length];
            for (int i = 0; i < inputValues.length; i++) {
                outputValues[i] = parseDateTime(inputValues[i]);
            }
        }
        return outputValues;
    }

    /**
     * Attempts to parse the given string value as a number attribute.
     *
     * @param stringValue   The string to be parsed.
     * @return              The number that the string represents.
     */
    private static Double parseNumberAttribute(String stringValue) {
        double parsedValue;
        try {
            parsedValue = new BigDecimal(stringValue).doubleValue();
        } catch(NumberFormatException ex) {
            try {
                DecimalFormat format = new DecimalFormat();
                Number number = format.parse(stringValue);
                parsedValue = number.doubleValue();
            } catch(ParseException exception) {
                ExceptionHelper.addSuppressed(exception, ex);
                throw new IllegalArgumentException("Unparseable number: \"" + stringValue + "\"", exception);
            }
        }
        return parsedValue;
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

        if (!IDataHelper.isEmpty(input)) {
            IDataCursor inputCursor = input.getCursor();
            IDataCursor outputCursor = output.getCursor();

            try {
                Set<String> names = getNames();

                while (inputCursor.next()) {
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
