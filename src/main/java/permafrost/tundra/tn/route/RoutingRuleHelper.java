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
import com.wm.app.b2b.server.ns.Namespace;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.app.tn.route.RoutingException;
import com.wm.app.tn.route.RoutingRule;
import com.wm.app.tn.route.RoutingRuleList;
import com.wm.app.tn.route.RoutingRuleStore;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.lang.ns.NSService;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.lang.StringHelper;
import permafrost.tundra.server.ServerLogHelper;
import permafrost.tundra.server.ServerLogLevel;
import permafrost.tundra.server.ServerLogStatement;
import permafrost.tundra.server.ServiceHelper;
import permafrost.tundra.server.UserHelper;
import permafrost.tundra.time.DurationHelper;
import permafrost.tundra.time.DurationPattern;
import permafrost.tundra.tn.document.BizDocEnvelopeHelper;
import permafrost.tundra.tn.util.TNFixedDataHelper;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Collection of convenience methods for working with routing rules.
 */
public class RoutingRuleHelper {
    /**
     * The default logging level used when logging.
     */
    private static final ServerLogLevel DEFAULT_LOG_LEVEL = ServerLogLevel.INFO;

    /**
     * Disallow instantiation of this class.
     */
    private RoutingRuleHelper() {}

    /**
     * Normalizes the given IData representation of a routing rule as a RoutingRule object.
     *
     * @param rule              An IData representation of a routing rule which includes either RuleID or RuleName.
     * @return                  The RoutingRule object for the given rule.
     * @throws RoutingException If no rule exists with the given identity or name, or if a data storage error occurs.
     */
    public static RoutingRule normalize(IData rule) throws RoutingException {
        if (rule == null) return null;

        RoutingRule routingRule;

        if (rule instanceof RoutingRule) {
            routingRule = (RoutingRule)rule;
        } else {
            IDataCursor cursor = rule.getCursor();
            try {
                String ruleID = IDataHelper.get(cursor, "RuleID", String.class);
                String ruleName = IDataHelper.get(cursor, "RuleName", String.class);

                if (ruleID == null && ruleName == null) {
                    throw new IllegalArgumentException("RuleID or RuleName is required");
                }

                routingRule = getByIdentityOrName(ruleID, ruleName, true);
            } finally {
                cursor.destroy();
            }
        }

        return routingRule;
    }

    /**
     * Returns the list of routing rules defined in Trading Networks.
     *
     * @return the list of routing rules defined in Trading Networks.
     */
    public static List<RoutingRule> list() {
        RoutingRuleList input = RoutingRuleStore.getList();
        List<RoutingRule> output = new ArrayList<RoutingRule>(input.size());
        for (Object rule : input.toArray()) {
            if (rule instanceof RoutingRule) {
                output.add((RoutingRule)rule);
            }
        }
        return output;
    }

    /**
     * Returns the routing rule with the given identity.
     *
     * @param ruleID    The rule identity.
     * @return          The rule with the given identity.
     */
    public static RoutingRule get(String ruleID) {
        return getByIdentityOrName(ruleID, null);
    }

    /**
     * Returns the routing rule with the given name.
     *
     * @param ruleName  The rule name.
     * @return          The rule with the given name.
     */
    public static RoutingRule getByName(String ruleName) {
        return getByIdentityOrName(null, ruleName);
    }

    /**
     * Returns the routing rule with the given identity or name.
     *
     * @param ruleID    The rule identity.
     * @param ruleName  The rule name.
     * @return          The rule with the given identity if specified, or with the given name.
     */
    public static RoutingRule getByIdentityOrName(String ruleID, String ruleName) {
        RoutingRule rule = null;
        try {
            rule = getByIdentityOrName(ruleID, ruleName, false);
        } catch(RoutingException ex) {
            // ignore exception, as it should never be thrown as raise argument is false
        }
        return rule;
    }

    /**
     * Returns the routing rule with the given identity or name.
     *
     * @param ruleID            The rule identity.
     * @param ruleName          The rule name.
     * @param raise             Whether to throw an exception if no rule exists with the given identity or name.
     * @return                  The rule with the given identity if specified, or with the given name.
     * @throws RoutingException If no rule exists with the given identity or name, or if a datastore error occurs.
     */
    public static RoutingRule getByIdentityOrName(String ruleID, String ruleName, boolean raise) throws RoutingException {
        RoutingRule rule = null;
        try {
            if (ruleID != null) {
                rule = RoutingRuleStore.getRule(ruleID);
            } else {
                rule = RoutingRuleStore.getRuleByName(ruleName);
            }
        } catch(RoutingException ex) {
            String message = ex.getMessage();
            if (raise || !message.matches("Processing rule \\S+ does not exist\\.")) {
                throw ex;
            }
        }
        return rule;
    }

    /**
     * Sets the given routing rule status to enabled.
     *
     * @param rule  The routing rule to set the status on.
     */
    public static void enable(RoutingRule rule) {
        setStatus(rule, true);
    }

    /**
     * Sets the given routing rule status to disabled.
     *
     * @param rule  The routing rule to set the status on.
     */
    public static void disable(RoutingRule rule) {
        setStatus(rule, false);
    }

    /**
     * Sets the status on the given routing rule.
     *
     * @param rule      The routing rule to set the status on.
     * @param enabled   Whether the routing rule status should be enabled or disabled.
     */
    private static void setStatus(RoutingRule rule, boolean enabled) {
        if (rule == null) return;

        try {
            rule.setDisabled(!enabled);
            RoutingRuleStore.updateStatus(rule);
        } catch(RoutingException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Returns the processing rule to be used to process the given bizdoc.
     *
     * @param bizdoc            The bizdoc to be processed.
     * @param parameters        The TN_parms routing hints to use.
     * @return                  The selected processing rule.
     * @throws ServiceException If an error occurs.
     */
    public static RoutingRule match(BizDocEnvelope bizdoc, IData parameters) throws ServiceException {
        return match(bizdoc, parameters, false);
    }

    /**
     * Returns the processing rule to be used to process the given bizdoc.
     *
     * @param bizdoc            The bizdoc to be processed.
     * @param parameters        The TN_parms routing hints to use.
     * @return                  The selected processing rule.
     * @throws ServiceException If an error occurs.
     */
    public static RoutingRule match(BizDocEnvelope bizdoc, IData parameters, boolean useActivityLog) throws ServiceException {
        if (bizdoc == null) throw new NullPointerException("bizdoc must not be null");

        RoutingRule rule = null;

        String ruleID = null, ruleName = null;

        if (parameters != null) {
            IDataCursor cursor = parameters.getCursor();
            try {
                ruleID = IDataHelper.get(cursor, "processingRuleID", String.class);
                ruleName = IDataHelper.get(cursor, "processingRuleName", String.class);
            } finally {
                cursor.destroy();
            }
        }

        if (ruleID != null) {
            rule = RoutingRuleStore.getRule(ruleID);
        } else if (ruleName != null) {
            rule = RoutingRuleStore.getRuleByName(ruleName);
        } else if (useActivityLog) {
            IData pipeline = IDataFactory.create();
            IDataCursor cursor = pipeline.getCursor();

            try {
                cursor.insertAfter("bizdoc", bizdoc);
                if (parameters != null) cursor.insertAfter("TN_parms", parameters);
                cursor.destroy();

                pipeline = Service.doInvoke("wm.tn.route", "getFirstMatch", pipeline);

                cursor = pipeline.getCursor();
                rule = IDataHelper.get(cursor, "rule", RoutingRule.class);
                cursor.destroy();
            } catch(Exception ex) {
                ExceptionHelper.raise(ex);
            } finally {
                cursor.destroy();
            }
        } else {
            rule = RoutingRuleStore.getFirstMatch(bizdoc);
        }

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
     * Whether the given rule is to be invoked asynchronously.
     *
     * @param rule  The rule to check.
     * @return      True if the rule is to be invoked asynchronously.
     */
    public static boolean isAsynchronous(RoutingRule rule) {
        return rule.getServiceInvokeType().equals("async");
    }

    /**
     * Whether the given rule is to be invoked reliably.
     *
     * @param rule  The rule to check.
     * @return      True if the rule is to be invoked reliably.
     */
    public static boolean isReliable(RoutingRule rule) {
        return rule.getServiceInvokeType().equals("reliable");
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
        if (rule == null) rule = match(bizdoc, parameters);

        Deferrer deferrer = Deferrer.getInstance();

        if (deferrer.isStarted() && (isAsynchronous(rule) || shouldDefer(parameters))) {
            deferrer.defer(bizdoc, rule, parameters);
        } else {
            route(rule, bizdoc, parameters);
        }
    }

    /**
     * The maximum supported length for the OriginalSenderID and OriginalReceiverID fields in a BizDocEnvelope.
     */
    private static final int ORIGINAL_SENDER_RECEIVER_ID_LENGTH = 140;

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
        long startTime = System.nanoTime();

        Throwable exception = null;
        IData pipeline = IDataFactory.create();

        if (isRoutable(parameters)) {
            // work around issue where persist fails if the OriginalSenderID or OriginalReceiverID exceeds the relevant
            // database table column size by truncating to that size
            String originalSenderID = bizdoc.getOriginalSenderId();
            if (originalSenderID != null && originalSenderID.length() > ORIGINAL_SENDER_RECEIVER_ID_LENGTH) {
                bizdoc.setOriginalSenderId(StringHelper.truncate(originalSenderID, ORIGINAL_SENDER_RECEIVER_ID_LENGTH));
            }
            String originalReceiverID = bizdoc.getOriginalReceiverId();
            if (originalReceiverID != null && originalReceiverID.length() > ORIGINAL_SENDER_RECEIVER_ID_LENGTH) {
                bizdoc.setOriginalReceiverId(StringHelper.truncate(originalReceiverID, ORIGINAL_SENDER_RECEIVER_ID_LENGTH));
            }

            IDataCursor cursor = pipeline.getCursor();
            try {
                cursor.insertAfter("rule", rule instanceof ImmutableRoutingRule ? rule : new ImmutableRoutingRule(rule));
                cursor.insertAfter("bizdoc", bizdoc);
                if (parameters != null) cursor.insertAfter("TN_parms", parameters);

                pipeline = Service.doInvoke("wm.tn.route", "route", pipeline);
            } catch(Exception ex) {
                exception = ex;
                ExceptionHelper.raise(ex);
            } finally {
                cursor.destroy();
                log(rule, bizdoc, System.nanoTime() - startTime, exception);
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
                isRoutable = !IDataHelper.getOrDefault(cursor, "$bypassRouting", Boolean.class, false);
            } finally {
                cursor.destroy();
            }
        }
        return isRoutable;
    }

    /**
     * Whether the given parameters include a hint to use deferred routing via a key named $deferredRouting with a value
     * of true.
     *
     * @param parameters    The optional TN_parms routing hints to use.
     * @return              True if routing should be deferred, false if not.
     */
    public static boolean shouldDefer(IData parameters) {
        boolean shouldDefer = false;
        if (parameters != null) {
            IDataCursor cursor = parameters.getCursor();
            try {
                shouldDefer = IDataHelper.getOrDefault(cursor, "$deferredRouting", Boolean.class, false);
            } finally {
                cursor.destroy();
            }
        }
        return shouldDefer;
    }

    private static final NSService WM_TN_ROUTE_ROUTE = (NSService)Namespace.current().getNode("wm.tn.route:route");

    /**
     * Logs that the given bizdoc was routed by the given rule.
     *
     * @param rule      The routing rule.
     * @param bizdoc    The bizdoc.
     * @param duration  The duration in nanoseconds for routing the given bizdoc.
     */
    private static void log(RoutingRule rule, BizDocEnvelope bizdoc, long duration, Throwable exception) {
        List<NSService> stack = ServiceHelper.getCallStack();
        stack.add(WM_TN_ROUTE_ROUTE);
        String message = MessageFormat.format("{0} -- routed document {1} {2}", ServerLogStatement.getFunction(UserHelper.getCurrentName(), stack, false), DurationHelper.format(duration, DurationPattern.NANOSECONDS, DurationPattern.XML_MILLISECONDS), exception == null ? "COMPLETED" : "FAILED: " + ExceptionHelper.getMessage(exception));
        ServerLogHelper.log(RoutingRuleHelper.class.getName(), DEFAULT_LOG_LEVEL, message, summarize(rule, bizdoc), false);
    }

    /**
     * Returns a summary of the given route, suitable for logging.
     *
     * @param rule      The rule to summarize.
     * @param bizdoc    The bizdoc to summarize.
     * @return          The summary of the route.
     */
    private static IData summarize(RoutingRule rule, BizDocEnvelope bizdoc) {
        IData summary = null;
        if (rule != null && bizdoc != null) {
            summary = IDataFactory.create();
            IDataCursor cursor = summary.getCursor();
            try {
                IDataHelper.put(cursor, "Rule", summarize(rule), false);
                IDataHelper.put(cursor, "Document", BizDocEnvelopeHelper.summarize(bizdoc), false);
            } finally {
                cursor.destroy();
            }
        }
        return summary;
    }

    /**
     * Returns a summary of the given object, suitable for logging.
     *
     * @param object    The object to summarize.
     * @return          The summary of the route.
     */
    private static IData summarize(RoutingRule object) {
        IData summary = null;
        if (object != null) {
            summary = IDataFactory.create();
            IDataCursor cursor = summary.getCursor();
            try {
                IDataHelper.put(cursor, "RuleID", object.getID());
                IDataHelper.put(cursor, "RuleName", object.getName(), false);
            } finally {
                cursor.destroy();
            }
        }
        return summary;
    }
}
