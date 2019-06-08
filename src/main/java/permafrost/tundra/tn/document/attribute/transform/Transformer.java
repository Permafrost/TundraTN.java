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

package permafrost.tundra.tn.document.attribute.transform;

import com.wm.data.IData;
import com.wm.data.IDataCursor;
import permafrost.tundra.data.IDataHelper;

/**
 * Abstract class for Trading Networks document attribute transformers.
 *
 * @param <T>           The class of the transformed values.
 */
public abstract class Transformer<T> {
    /**
     * Transforms the given Trading Networks extracted document attribute values.
     *
     * @param values    The extracted document attribute values to transform.
     * @param isArray   Whether there are multiple values to be transformed.
     * @param arg       The modifying argument for the transformation, if applicable.
     * @return          The transformed document attribute values.
     */
    public abstract T[] transform(String[] values, boolean isArray, String arg);

    /**
     * Transforms the given Trading Networks document attribute transformation pipeline.
     *
     * @param pipeline      The pipeline to apply the transformation to.
     * @return              The transformed pipeline.
     */
    public IData transform(IData pipeline) {
        if (pipeline == null) return null;

        IDataCursor cursor = pipeline.getCursor();

        try {
            String[] values = IDataHelper.get(cursor, "values", String[].class);
            boolean isArray = IDataHelper.get(cursor, "isArray", Boolean.class);
            String arg = IDataHelper.get(cursor, "arg", String.class);

            IDataHelper.put(cursor, "newValues", transform(values, isArray, arg));
        } finally {
            cursor.destroy();
        }

        return pipeline;
    }

    /**
     * Apply the given transformer to the given Trading Networks document attribute transformation pipeline.
     *
     * @param pipeline      The pipeline to apply the transformation to.
     * @param transformer   The transformer to use.
     * @param <T>           The class of the transformed values.
     * @return              The transformed pipeline.
     */
    public static <T> IData transform(IData pipeline, Transformer<T> transformer) {
        if (transformer == null) return pipeline;
        return transformer.transform(pipeline);
    }
}
