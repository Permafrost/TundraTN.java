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
import permafrost.tundra.tn.util.TNFixedDataHelper;

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
     * @throws ServiceException If an error occurs.
     */
    public static RoutingRule select(BizDocEnvelope bizdoc, IData parameters) throws ServiceException {
        return select(bizdoc, parameters, false);
    }

    /**
     * Returns the processing rule to be used to process the given bizdoc.
     *
     * @param bizdoc            The bizdoc to be processed.
     * @param parameters        The TN_parms routing hints to use.
     * @return                  The selected processing rule.
     * @throws ServiceException If an error occurs.
     */
    public static RoutingRule select(BizDocEnvelope bizdoc, IData parameters, boolean useActivityLog) throws ServiceException {
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

        if (ruleID == null && ruleName == null) {
            if (useActivityLog) {
                IData pipeline = IDataFactory.create();
                IDataCursor cursor = pipeline.getCursor();

                try {
                    cursor.insertAfter("bizdoc", bizdoc);
                    if (parameters != null) cursor.insertAfter("TN_parms", parameters);
                    cursor.destroy();

                    pipeline = Service.doInvoke("wm.tn.route", "getFirstMatch", pipeline);

                    cursor = pipeline.getCursor();
                    rule = (RoutingRule)IDataUtil.get(cursor, "rule");
                    cursor.destroy();
                } catch(Exception ex) {
                    ExceptionHelper.raise(ex);
                } finally {
                    cursor.destroy();
                }
            } else {
                rule = RoutingRuleStore.getFirstMatch(bizdoc);
            }
        } else if (ruleID != null) {
            rule = RoutingRuleStore.getRule(ruleID);
        } else {
            rule = RoutingRuleStore.getRuleByName(ruleName);
        }

        // duplicate the rule to avoid race conditions with multiple threads accessing the same rule object
        return rule;
    }

    /**
     * Makes a field for field copy of the given routing rule.
     *
     * @param rule  The routing rule to clone.
     * @return      A duplicate of the given routing rule.
     */
    public static RoutingRule duplicate(RoutingRule rule) {
        return TNFixedDataHelper.duplicate(rule);
    }

    /**
     * Whether the given rule is to be invoked synchronously.
     *
     * @param rule  The rule to check.
     * @return      True if the rule is to be invoked synchronously.
     */
    public static boolean isSynchronous(RoutingRule rule) {
        return rule.getServiceInvokeType().equals("sync");
    }

    /**
     * Processes the given bizdoc using the given rule.
     *
     * @param rule              The rule to use.
     * @param bizdoc            The bizdoc to process.
     * @param parameters        The TN_parms routing hints to use.
     * @throws ServiceException If an error occurs while processing.
     */
    public static void execute(RoutingRule rule, BizDocEnvelope bizdoc, IData parameters) throws ServiceException {
        if (bizdoc == null) throw new NullPointerException("bizdoc must not be null");
        if (rule == null) rule = select(bizdoc, parameters);

        Deferrer deferrer = Deferrer.getInstance();

        if (isSynchronous(rule) || !deferrer.isStarted()) {
            route(rule, bizdoc, parameters);
        } else {
            deferrer.defer(new CallableRoute(bizdoc, rule, parameters));
        }
    }

    /**
     * Routes the given bizdoc using the given rule.
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
