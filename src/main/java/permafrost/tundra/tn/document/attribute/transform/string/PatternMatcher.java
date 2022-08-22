/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2022 Lachlan Dowding
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

package permafrost.tundra.tn.document.attribute.transform.string;

import permafrost.tundra.lang.BooleanHelper;
import permafrost.tundra.tn.document.attribute.transform.Transformer;
import permafrost.tundra.util.regex.PatternHelper;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

abstract public class PatternMatcher extends Transformer<String> {
    /**
     * If true will short circuit evaluation as a logical AND operation, otherwise as a logical OR operator.
     */
    protected boolean shortCircuitLogicalAnd = true;

    /**
     * Transforms the given Trading Networks extracted document attribute values.
     *
     * @param values  The extracted document attribute values to transform.
     * @param isArray Whether there are multiple values to be transformed.
     * @param arg     The modifying argument for the transformation, if applicable.
     * @return The transformed document attribute values.
     */
    @Override
    public String[] transform(String[] values, boolean isArray, String arg) {
        String[] output = new String[1];

        if (values != null && arg != null) {
            boolean result = false;
            Pattern pattern = PatternHelper.compile(arg);

            Set<String> uniqueValues;
            if (values.length > 1) {
                uniqueValues = new HashSet<String>(values.length);
            } else {
                uniqueValues = null;
            }

            for (int i = 0; i < values.length; i++) {
                if (uniqueValues == null || uniqueValues.add(values[i])) {
                    result = evaluate(values[i], pattern);
                }
                if (shortCircuitLogicalAnd && !result) {
                    break;
                } else if (!shortCircuitLogicalAnd && result) {
                    break;
                }
            }

            output[0] = BooleanHelper.emit(result);
        } else {
            output[0] = "false";
        }

        return output;
    }

    /**
     * Evaluates the given value against the given pattern returning a boolean result.
     *
     * @param value     The value to be evaluated.
     * @param pattern   The pattern to evaluate the value against.
     * @return          The boolean result of the evaluation.
     */
    abstract protected boolean evaluate(String value, Pattern pattern);
}
