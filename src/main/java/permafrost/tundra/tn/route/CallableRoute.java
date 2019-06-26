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

import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.app.tn.route.RoutingRule;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.lang.ThreadHelper;
import permafrost.tundra.time.DateTimeHelper;
import permafrost.tundra.tn.document.BizDocEnvelopeHelper;
import permafrost.tundra.util.concurrent.AbstractPrioritizedCallable;
import java.math.BigDecimal;
import java.text.MessageFormat;

/**
 * Used to defer processing a bizdoc to another thread.
 */
public class CallableRoute extends AbstractPrioritizedCallable<IData> {
    /**
     * The document attribute name for route priority.
     */
    public static final String MESSAGE_PRIORITY_ATTRIBUTE_NAME = "Message Priority";
    /**
     * The document attribute name for thread priority.
     */
    public static final String THREAD_PRIORITY_ATTRIBUTE_NAME = "Thread Priority";
    /**
     * Document user statuses set or used by this class.
     */
    public static final String BIZDOC_USER_STATUS_DEFERRED = "DEFERRED";
    public static final String BIZDOC_USER_STATUS_ROUTING = "ROUTING";
    public static final String BIZDOC_USER_STATUS_DONE = "DONE";
    public static final String BIZDOC_USER_STATUS_ERROR = "ERROR";
    /**
     * The internal ID of the bizdoc to be routed.
     */
    protected String id;
    /**
     * The bizdoc to be routed.
     */
    protected BizDocEnvelope bizdoc;
    /**
     * The rule to use when routing.
     */
    protected RoutingRule rule;
    /**
     * The TN_parms to use when routing.
     */
    protected IData parameters;
    /**
     * The thread priority to use when executing this route.
     */
    protected int threadPriority = Thread.NORM_PRIORITY;

    /**
     * Constructs a new CallableRoute.
     *
     * @param id    The internal ID of the bizdoc to be routed.
     */
    public CallableRoute(String id) throws ServiceException {
        this(BizDocEnvelopeHelper.get(id, true));
    }

    /**
     * Constructs a new CallableRoute.
     *
     * @param bizdoc    The bizdoc to be routed.
     */
    public CallableRoute(BizDocEnvelope bizdoc) throws ServiceException {
        this(bizdoc, null);
    }

    /**
     * Constructs a new CallableRoute.
     *
     * @param bizdoc    The bizdoc to be routed.
     * @param rule      The rule to use when routing.
     */
    public CallableRoute(BizDocEnvelope bizdoc, RoutingRule rule) throws ServiceException {
        this(bizdoc, rule, null);
    }

    /**
     * Constructs a new CallableRoute.
     *
     * @param bizdoc     The bizdoc to be routed.
     * @param rule       The rule to use when routing.
     * @param parameters The optional TN_parms to use when routing.
     */
    public CallableRoute(BizDocEnvelope bizdoc, RoutingRule rule, IData parameters) throws ServiceException {
        if (bizdoc == null) throw new NullPointerException("bizdoc must not be null");
        if (rule == null) rule = RoutingRuleHelper.select(bizdoc, parameters);

        this.id = bizdoc.getInternalId();
        this.bizdoc = bizdoc;
        // if rule is not synchronous, change it to be synchronous since it's already being executed
        // asynchronously as a deferred route so we don't want it to spawn yet another thread
        this.rule = rule.getServiceInvokeType().equals("sync") ? rule : new SynchronousRoutingRule(rule);
        this.parameters = IDataHelper.duplicate(parameters);

        IData attributes = bizdoc.getAttributes();
        if (attributes != null) {
            IDataCursor cursor = attributes.getCursor();
            try {
                BigDecimal messagePriority = IDataHelper.get(cursor, MESSAGE_PRIORITY_ATTRIBUTE_NAME, BigDecimal.class);
                if (messagePriority != null) {
                    this.priority = messagePriority.doubleValue();
                }

                BigDecimal threadPriority = IDataHelper.get(cursor, THREAD_PRIORITY_ATTRIBUTE_NAME, BigDecimal.class);
                if (threadPriority != null) {
                    this.threadPriority = ThreadHelper.normalizePriority(threadPriority.intValue());
                }
            } finally {
                cursor.destroy();
            }
        }

        if (!BIZDOC_USER_STATUS_DEFERRED.equals(bizdoc.getUserStatus())) {
            BizDocEnvelopeHelper.setStatus(bizdoc, null, BIZDOC_USER_STATUS_DEFERRED);
        }
    }

    /**
     * Returns the thread priority this route should be executed with.
     *
     * @return the thread priority this route should be executed with.
     */
    public int getThreadPriority() {
        return threadPriority;
    }

    /**
     * Routes the bizdoc.
     */
    @Override
    public IData call() throws ServiceException {
        IData output;

        Thread currentThread = Thread.currentThread();
        String currentThreadName = currentThread.getName();
        int currentThreadPriority = currentThread.getPriority();

        String doneStatus = BIZDOC_USER_STATUS_DONE;
        boolean wasRouted = false;

        try {
            currentThread.setName(MessageFormat.format("{0}: BizDoc {1} PROCESSING {2}", currentThreadName, BizDocEnvelopeHelper.toLogString(bizdoc), DateTimeHelper.now("datetime")));
            currentThread.setPriority(getThreadPriority());

            if (BizDocEnvelopeHelper.setStatus(id, null, null, BIZDOC_USER_STATUS_ROUTING, BIZDOC_USER_STATUS_DEFERRED, false)) {
                // status was able to be changed, so we have a "lock" on the bizdoc and can now route it
                wasRouted = true;

                output = RoutingRuleHelper.route(rule, bizdoc, parameters);

                if (BizDocEnvelopeHelper.hasErrors(bizdoc)) {
                    doneStatus = doneStatus + " W/ ERRORS";
                }

                BizDocEnvelopeHelper.setStatus(bizdoc, null, null, doneStatus, BIZDOC_USER_STATUS_ROUTING, false);
            } else {
                output = IDataFactory.create();
            }
        } catch(ServiceException ex) {
            if (wasRouted) BizDocEnvelopeHelper.setStatus(bizdoc, null, null, BIZDOC_USER_STATUS_ERROR, BIZDOC_USER_STATUS_ROUTING, false);
            throw ex;
        } finally {
            currentThread.setName(currentThreadName);
            currentThread.setPriority(currentThreadPriority);
        }

        return output;
    }

    /**
     * Returns a string representation of this object.
     *
     * @return a string representation of this object.
     */
    @Override
    public String toString() {
        return super.toString() + " BizDoc [ID=" + id + "]";
    }
}