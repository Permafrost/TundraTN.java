/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Lachlan Dowding
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

import com.wm.app.tn.profile.ProfileStoreException;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.tn.document.attribute.transform.Transformer;
import permafrost.tundra.tn.profile.ProfileID;

/**
 * Transforms the given Trading Networks partner profile external identities to internal identities.
 */
public class ProfileFinder extends Transformer<String> {
    /**
     * Transforms the given Trading Networks extracted document attribute values.
     *
     * @param values    The extracted document attribute values to transform.
     * @param isArray   Whether there are multiple values to be transformed.
     * @param arg       The modifying argument for the transformation, if applicable.
     * @return          The transformed document attribute values.
     */
    @Override
    public String[] transform(String[] values, boolean isArray, String arg) {
        String[] output = new String[values.length];

        try {
            for (int i = 0; i < values.length; i++) {
                ProfileID externalProfileID = new ProfileID(values[i], arg);
                ProfileID internalProfileID = externalProfileID.toInternalID();
                if (internalProfileID != null) {
                    output[i] = internalProfileID.getValue();
                }
            }
        } catch(ProfileStoreException ex) {
            ExceptionHelper.raiseUnchecked(ex);
        }

        return output;
    }
}
