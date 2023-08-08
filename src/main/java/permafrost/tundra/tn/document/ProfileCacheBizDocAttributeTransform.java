/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2023 Lachlan Dowding
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

import com.wm.app.tn.doc.BizDocAttributeTransform;
import permafrost.tundra.tn.profile.ExternalID;
import permafrost.tundra.tn.profile.InternalID;

/**
 * A BizDocAttributeTransform that uses the TundraTN profile cache when resolving partner IDs, but delegates all other
 * functions to another given BizDocAttributeTransform object.
 */
public class ProfileCacheBizDocAttributeTransform extends ProxyBizDocAttributeTransform {
    /**
     * Constructs a new ProfileCacheBizDocAttributeTransform that delegates to the given BizDocAttributeTransform for
     * all functions except resolving a partner ID, in which case the TundraTN profile cache will be used to attempt
     * to resolve the partner to avoid unnecessary database/network access and work around the issue where TN suppresses
     * database/network errors and defaults the sender and/or receiver to the Unknown profile.
     *
     * @param transformer   The BizDocAttributeTransform method calls will be delegated to.
     */
    public ProfileCacheBizDocAttributeTransform(BizDocAttributeTransform transformer) {
        super(transformer);
    }

    /**
     * Uses the TundraTN profile cache to lookup partner IDs to work around issue where loss of network or database
     * connection during BizDoc recognize process results in Trading Networks suppressing the error and setting the
     * sender and/or receiver to the Unknown profile. If the required partner is missing from the TundraTN cache, it
     * will be looked up in the database, however unlike the built-in TN process, this method will throw any exceptions
     * that occur during that look up, rather than suppressing them and defaulting to the Unknown profile.
     *
     * @param values        The values to be applied.
     * @return              The applied value.
     * @throws Exception    If an error occurs.
     */
    public Object apply(String[] values) throws Exception {
        Object result = null;
        if (getFunction() == BizDocAttributeTransform.FN_PARTNER_LOOKUP) {
            String externalId;
            if (getPreFunction() != -1 && getPreFunction() != getFunction()) {
                BizDocAttributeTransform preFunctionTransformer = new BizDocAttributeTransform(getAttribute(), getPreFunction(), getPreFnArgs());
                externalId = (String)preFunctionTransformer.apply(values);
            } else {
                externalId = values[0];
            }

            String[] args = getArgs();
            if (args != null && args.length > 0) {
                int externalIDType = Integer.parseInt(args[0]);
                ExternalID externalID = new ExternalID(externalId, externalIDType);
                InternalID internalID = externalID.intern();
                if (internalID != null) {
                    result = internalID.getIdentity();
                }
            }
        } else {
            result = super.apply(values);
        }
        return result;
    }
}
