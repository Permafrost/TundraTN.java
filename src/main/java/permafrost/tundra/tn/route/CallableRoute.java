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
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.lang.ThreadHelper;
import permafrost.tundra.tn.document.BizDocEnvelopeHelper;
import permafrost.tundra.util.concurrent.AbstractPrioritizedCallable;
import java.math.BigDecimal;

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
     * Bizdoc user status set when routing is completed successfully.
     */
    public static final String BIZDOC_USER_STATUS_DONE = "DONE";
    /**
     * Bizdoc user status set when routing failed.
     */
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
     * Whether this route requires initialization to be run.
     */
    protected volatile boolean requiresInitialization = true;

    /**
     * Constructs a new CallableRoute.
     *
     * @param id                The internal ID of the bizdoc to be routed.
     */
    public CallableRoute(String id) {
        if (id == null) throw new NullPointerException("id must not be null");
        this.id = id;
    }

    /**
     * Constructs a new CallableRoute.
     *
     * @param bizdoc            The bizdoc to be routed.
     * @param rule              The rule to use when routing.
     * @param parameters        The optional TN_parms to use when routing.
     * @throws ServiceException If an error occurs.
     */
    public CallableRoute(BizDocEnvelope bizdoc, RoutingRule rule, IData parameters) throws ServiceException {
        if (bizdoc == null) throw new NullPointerException("bizdoc must not be null");

        this.id = bizdoc.getInternalId();
        this.bizdoc = bizdoc;
        this.rule = rule;
        this.parameters = IDataHelper.duplicate(parameters);
    }

    /**
     * Initializes this route.
     *
     * @throws ServiceException If an error occurs.
     */
    protected synchronized void initialize() throws ServiceException {
        if (requiresInitialization) {
            requiresInitialization = false;

            if (id == null && bizdoc == null) throw new NullPointerException("bizdoc must not be null");

            if (bizdoc == null) {
                bizdoc = BizDocEnvelopeHelper.get(id, true);
            }

            if (rule == null) rule = RoutingRuleHelper.select(bizdoc, parameters);
            // if rule is not synchronous, change it to be synchronous since it's already being executed
            // asynchronously as a deferred route so we don't want it to spawn yet another thread
            rule = rule.getServiceInvokeType().equals("sync") ? rule : new SynchronousRoutingRule(rule);

            IData attributes = bizdoc.getAttributes();
            if (attributes != null) {
                IDataCursor cursor = attributes.getCursor();
                try {
                    BigDecimal messagePriority = IDataHelper.get(cursor, MESSAGE_PRIORITY_ATTRIBUTE_NAME, BigDecimal.class);
                    if (messagePriority != null) {
                        this.priority = messagePriority.doubleValue();
                    }

                    BigDecimal threadPriorityAttribute = IDataHelper.get(cursor, THREAD_PRIORITY_ATTRIBUTE_NAME, BigDecimal.class);
                    if (threadPriorityAttribute != null) {
                        threadPriority = ThreadHelper.normalizePriority(threadPriorityAttribute.intValue());
                    }
                } finally {
                    cursor.destroy();
                }
            }
        }
    }

    /**
     * Returns the identity of the bizdoc this route is executed against.
     *
     * @return the identity of the bizdoc this route is executed against.
     */
    public String getIdentity() {
        return id;
    }

    /**
     * Returns the priority of this object, where larger values are higher priority, and with a nominal range of 1..10.
     *
     * @return the priority of this object, where larger values are higher priority, and with a nominal range of 1..10.
     */
    @Override
    public double getPriority() {
        try {
            initialize();
        } catch(ServiceException ex) {
            // do nothing
        }
        return super.getPriority();
    }

    /**
     * Routes the bizdoc.
     *
     * @throws ServiceException If an error occurs.
     */
    @Override
    public IData call() throws ServiceException {
        IData output;

        Thread currentThread = Thread.currentThread();
        int previousThreadPriority = currentThread.getPriority();

        // lazy initialization so route objects can be constructed from id as quickly as possible
        initialize();

        currentThread.setPriority(threadPriority);

        String previousStatus = bizdoc.getUserStatus();
        String doneStatus = BIZDOC_USER_STATUS_DONE;

        try {
            output = RoutingRuleHelper.route(rule, bizdoc, parameters);

            if (BizDocEnvelopeHelper.hasErrors(bizdoc)) {
                doneStatus = doneStatus + " W/ ERRORS";
            }

            BizDocEnvelopeHelper.setUserStatusForPrevious(bizdoc, doneStatus, previousStatus);
        } catch(ServiceException ex) {
            BizDocEnvelopeHelper.setUserStatusForPrevious(bizdoc, BIZDOC_USER_STATUS_ERROR, previousStatus);
            throw ex;
        } finally {
            currentThread.setPriority(previousThreadPriority);
        }

        return output;
    }
}