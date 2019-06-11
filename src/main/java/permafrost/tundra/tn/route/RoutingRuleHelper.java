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

package permafrost.tundra.tn.route;

import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.app.tn.route.RoutingRule;
import com.wm.app.tn.route.RoutingRuleStore;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.data.IDataUtil;
import permafrost.tundra.lang.ExceptionHelper;

/**
 * Collection of convenience methods for working with routing rules.
 */
public class RoutingRuleHelper {
    /**
     * Disallow instantiation of this class.
     */
    private RoutingRuleHelper() {}

    /**
     * Returns the processing rule to be used to process the given bizdoc.
     *
     * @param bizdoc            The bizdoc to be processed.
     * @param parameters        The TN_parms routing hints to use.
     * @return                  The selected processing rule.
     */
    public static RoutingRule select(BizDocEnvelope bizdoc, IData parameters) {
        RoutingRule rule = null;

        String ruleID = null, ruleName = null;

        if (parameters != null) {
            IDataCursor cursor = parameters.getCursor();

            try {
                ruleID = IDataUtil.getString(cursor, "processingRuleID");
                ruleName = IDataUtil.getString(cursor, "processingRuleName");
            } finally {
                cursor.destroy();
            }
        }

        try {
            if (ruleID == null && ruleName == null) {
                rule = RoutingRuleStore.getFirstMatch(bizdoc);
            } else if (ruleID != null) {
                rule = RoutingRuleStore.getRule(ruleID);
            } else {
                rule = RoutingRuleStore.getRuleByName(ruleName);
            }
        } catch(ServiceException ex) {
            throw new RuntimeException(ex);
        }

        return rule;
    }

    /**
     * Processes the given bizdoc using the given rule.
     *
     * @param rule              The rule to use to process the bizdoc.
     * @param bizdoc            The bizdoc to be processed.
     * @param parameters        The optional TN_parms routing hints to use.
     * @return                  The output pipeline returned from routing the bizdoc.
     * @throws ServiceException If an error occurs invoking wm.tn.route:route.
     */
    public static IData route(RoutingRule rule, BizDocEnvelope bizdoc, IData parameters) throws ServiceException {
        IData pipeline = IDataFactory.create();

        if (isRoutable(parameters)) {
            IDataCursor cursor = pipeline.getCursor();
            try {
                cursor.insertAfter("rule", rule);
                cursor.insertAfter("bizdoc", bizdoc);
                if (parameters != null) cursor.insertAfter("TN_parms", parameters);

                pipeline = Service.doInvoke("wm.tn.route", "route", pipeline);
            } catch(Exception ex) {
                ExceptionHelper.raise(ex);
            } finally {
                cursor.destroy();
            }
        }

        return pipeline;
    }

    /**
     * Whether the given parameters allow routing or not. Routing is disallowed when the given parameters contain a
     * key named $bypassRouting with a value of true.
     *
     * @param parameters    The optional TN_parms routing hints to use.
     * @return              True if routing is allowed, false if not.
     */
    public static boolean isRoutable(IData parameters) {
        boolean isRoutable = true;
        if (parameters != null) {
            IDataCursor cursor = parameters.getCursor();
            try {
                String bypassRouting = IDataUtil.getString(cursor, "$bypassRouting");
                if (bypassRouting != null) {
                    isRoutable = Boolean.parseBoolean(bypassRouting);
                }
            } finally {
                cursor.destroy();

            }
        }
        return isRoutable;
    }
}
