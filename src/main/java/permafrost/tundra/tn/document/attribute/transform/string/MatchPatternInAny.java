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

import permafrost.tundra.lang.StringHelper;
import java.util.regex.Pattern;

/**
 * Transforms the given String[] to a single boolean indicating if the given regular expression pattern matched any
 * of the array elements.
 */
public class MatchPatternInAny extends PatternMatcher {
    /**
     * Construct a new MatchPatternInAny object.
     */
    public MatchPatternInAny() {
        shortCircuitLogicalAnd = false;
    }
    /**
     * Evaluates the given value against the given pattern returning a boolean result.
     *
     * @param value     The value to be evaluated.
     * @param pattern   The pattern to evaluate the value against.
     * @return          The boolean result of the evaluation.
     */
    @Override
    protected boolean evaluate(String value, Pattern pattern) {
        return StringHelper.match(value, pattern);
    }
}
