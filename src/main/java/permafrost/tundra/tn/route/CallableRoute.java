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
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.tn.document.BizDocEnvelopeHelper;
import permafrost.tundra.tn.document.BizDocEnvelopePriority;
import permafrost.tundra.util.concurrent.AbstractPrioritizedCallable;
import permafrost.tundra.util.concurrent.Priority;

/**
 * Used to defer processing a bizdoc to another thread.
 */
public class CallableRoute extends AbstractPrioritizedCallable<IData> {
    /**
     * The default message priority used when messages do not specify a message priority.
     */
    public static final double DEFAULT_MESSAGE_PRIORITY = 5.0d;
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

            if (rule == null) rule = RoutingRuleHelper.match(bizdoc, parameters);
            // if rule is asynchronous, change it to be synchronous since it's already being executed
            // asynchronously as a deferred route so we don't want it to spawn yet another thread
            rule = RoutingRuleHelper.isAsynchronous(rule) ? new SynchronousRoutingRule(rule) : rule;

            priority = new BizDocEnvelopePriority(bizdoc);
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
     * Returns the priority of this object.
     *
     * @return the priority of this object.
     */
    @Override
    public Priority getPriority() {
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

        // escalate or de-escalate thread priority if required
        if (priority instanceof BizDocEnvelopePriority) {
            currentThread.setPriority(((BizDocEnvelopePriority)priority).getThreadPriority());
        }

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