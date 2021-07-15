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

package permafrost.tundra.tn.log;

public enum EntryType {
    ERROR(0), WARNING(1), MESSAGE(2);

    /**
     * The default entry type used when no type is specified.
     */
    public static final EntryType DEFAULT_ENTRY_TYPE = MESSAGE;
    /**
     * The entry type's value used when logging to the Trading Network's Activity Log.
     */
    private final int value;

    /**
     * Constructs a new EntryType.
     *
     * @param value The value used when logging to the Trading Network's Activity Log.
     */
    EntryType(int value) {
        this.value = value;
    }

    /**
     * Returns the value associated with this EntryType.
     * @return the value associated with this EntryType.
     */
    public int getValue() {
        return this.value;
    }

    /**
     * Returns the EntryType for the given string.
     *
     * @param entryType The entry type as a string.
     * @return          The corresponding enumeration value.
     */
    public static EntryType normalize(String entryType) {
        EntryType output;
        if (entryType == null) {
            output = DEFAULT_ENTRY_TYPE;
        } else {
            output = valueOf(entryType.trim().toUpperCase());
        }

        return output;
    }

    /**
     * Returns the EntryType for the given string.
     *
     * @param entryType The entry type as a string.
     * @return          The corresponding enumeration value.
     */
    public static EntryType normalize(EntryType entryType) {
        return entryType == null ? DEFAULT_ENTRY_TYPE : entryType;
    }
}
