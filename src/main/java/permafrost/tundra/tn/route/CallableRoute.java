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

import com.wm.app.b2b.server.InvokeState;
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.b2b.server.StateManager;
import com.wm.app.b2b.server.User;
import com.wm.app.tn.db.DatastoreException;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.app.tn.route.RoutingRule;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.util.coder.IDataCodable;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.lang.ThreadHelper;
import permafrost.tundra.server.SessionHelper;
import permafrost.tundra.server.UserHelper;
import permafrost.tundra.tn.document.BizDocEnvelopeHelper;
import permafrost.tundra.util.concurrent.AbstractPrioritizedCallable;
import java.math.BigDecimal;

/**
 * Used to defer processing a bizdoc to another thread.
 */
public class CallableRoute extends AbstractPrioritizedCallable<IData> implements IDataCodable {
    /**
     * The document attribute name for route priority.
     */
    public static final String MESSAGE_PRIORITY_ATTRIBUTE_NAME = "Message Priority";
    /**
     * The document attribute name for thread priority.
     */
    public static final String THREAD_PRIORITY_ATTRIBUTE_NAME = "Thread Priority";
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
    public CallableRoute(String id) {
        if (id == null) throw new NullPointerException("id must not be null");
        this.id = id;
    }

    /**
     * Constructs a new CallableRoute.
     *
     * @param bizdoc    The bizdoc to be routed.
     */
    public CallableRoute(BizDocEnvelope bizdoc) {
        this(bizdoc, null);
    }

    /**
     * Constructs a new CallableRoute.
     *
     * @param bizdoc    The bizdoc to be routed.
     * @param rule      The rule to use when routing.
     */
    public CallableRoute(BizDocEnvelope bizdoc, RoutingRule rule) {
        this(bizdoc, rule, null);
    }

    /**
     * Constructs a new CallableRoute.
     *
     * @param bizdoc     The bizdoc to be routed.
     * @param rule       The rule to use when routing.
     * @param parameters The optional TN_parms to use when routing.
     */
    public CallableRoute(BizDocEnvelope bizdoc, RoutingRule rule, IData parameters) {
        if (bizdoc == null) throw new NullPointerException("bizdoc must not be null");

        this.id = bizdoc.getInternalId();
        this.bizdoc = bizdoc;
        this.rule = rule;
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
     * Initialize the invoke state required to route the bizdoc.
     *
     * @return The newly created session's ID.
     */
    protected String initialize() {
        InvokeState currentState = new InvokeState();

        User user = UserHelper.administrator();
        if (user == null) {
            user = currentState.getUser();
        }
        currentState.setSession(SessionHelper.create(Thread.currentThread().getName(), user));
        currentState.setUser(user);
        currentState.setCheckAccess(false);

        InvokeState.setCurrentState(currentState);
        InvokeState.setCurrentSession(currentState.getSession());
        InvokeState.setCurrentUser(currentState.getUser());

        return currentState.getSession().getSessionID();
    }

    /**
     * Loads the bizdoc from the database, and selects the processing rule to run, if required.
     *
     * @throws DatastoreException   If an error loading the bizdoc occurs.
     */
    protected void preroute() throws DatastoreException {
        if (bizdoc == null) {
            bizdoc = BizDocEnvelopeHelper.get(id, true);
        }
        if (rule == null) {
            rule = RoutingRuleHelper.select(bizdoc, parameters);
        }
    }

    /**
     * Routes the bizdoc.
     */
    @Override
    public IData call() throws ServiceException {
        IData output;

        Thread currentThread = Thread.currentThread();
        String currentThreadName = currentThread.getName();

        InvokeState previousState = InvokeState.getCurrentState();
        String sessionID = null;

        try {
            currentThread.setName(currentThreadName + " Processing BizDoc/InternalID=" + id);
            sessionID = initialize();

            if (BizDocEnvelopeHelper.setStatus(id, null, null, "ROUTING", "DEFERRED", false)) {
                // load bizdoc from database, and select processing rule, if required
                preroute();

                // if rule is not synchronous, change it to be synchronous since it's already being executed
                // asynchronously as a deferred route
                if (!rule.getServiceInvokeType().equals("sync")) {
                    rule = (RoutingRule)rule.clone();
                    rule.setService(rule.getServiceName(), rule.getServiceInput(), "sync");
                }

                // status was able to be changed, so we have a "lock" on the bizdoc and can now route it
                output = RoutingRuleHelper.route(rule, bizdoc, parameters);
            } else {
                output = IDataFactory.create();
            }
        } finally {
            currentThread.setName(currentThreadName);
            InvokeState.setCurrentState(previousState);
            StateManager.deleteContext(sessionID);
        }

        return output;
    }

    /**
     * Returns an IData representation of this object.
     *
     * @return an IData representation of this object.
     */
    @Override
    public IData getIData() {
        IData document = IDataFactory.create();
        IDataCursor cursor = document.getCursor();

        try {
            cursor.insertAfter("id", id);
            cursor.insertAfter("bizdoc", bizdoc);
            cursor.insertAfter("rule", rule);
            if (parameters != null) cursor.insertAfter("TN_parms", parameters);
        } finally {
            cursor.destroy();
        }

        return document;
    }

    /**
     * Returns a string representation of this object.
     *
     * @return a string representation of this object.
     */
    @Override
    public String toString() {
        return super.toString() + " BizDoc/InternalID=" + id;
    }

    /**
     * This method is not implemented.
     *
     * @param document  An IData document.
     */
    @Override
    public void setIData(IData document) {
        throw new UnsupportedOperationException("setIData(IData) is not implemented");
    }
}