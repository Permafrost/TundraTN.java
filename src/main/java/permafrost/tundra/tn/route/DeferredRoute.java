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
import permafrost.tundra.server.SessionHelper;
import permafrost.tundra.server.UserHelper;
import permafrost.tundra.tn.document.BizDocEnvelopeHelper;

/**
 * Used to defer processing a bizdoc to another thread.
 */
public class DeferredRoute implements Runnable, IDataCodable {
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
     * Constructs a new DeferredRoute.
     *
     * @param id    The internal ID of the bizdoc to be routed.
     */
    public DeferredRoute(String id) throws DatastoreException {
        this(BizDocEnvelopeHelper.get(id, true));
    }

    /**
     * Constructs a new DeferredRoute.
     *
     * @param bizdoc    The bizdoc to be routed.
     */
    public DeferredRoute(BizDocEnvelope bizdoc) {
        this(bizdoc, RoutingRuleHelper.select(bizdoc, null));
    }

    /**
     * Constructs a new DeferredRoute.
     *
     * @param bizdoc    The bizdoc to be routed.
     * @param rule      The rule to use when routing.
     */
    public DeferredRoute(BizDocEnvelope bizdoc, RoutingRule rule) {
        this(bizdoc, rule, null);
    }

    /**
     * Constructs a new DeferredRoute.
     *
     * @param bizdoc     The bizdoc to be routed.
     * @param rule       The rule to use when routing.
     * @param parameters The optional TN_parms to use when routing.
     */
    public DeferredRoute(BizDocEnvelope bizdoc, RoutingRule rule, IData parameters) {
        if (bizdoc == null) throw new NullPointerException("bizdoc must not be null");
        if (rule == null) throw new NullPointerException("rule must not be null");

        this.bizdoc = bizdoc;
        this.rule = rule;
        this.parameters = IDataHelper.duplicate(parameters);
    }

    /**
     * Routes the bizdoc.
     */
    @Override
    public void run() {
        Thread currentThread = Thread.currentThread();
        String originalThreadName = currentThread.getName();

        currentThread.setName(originalThreadName + " Processing BizDoc/InternalID = " + bizdoc.getInternalId());

        InvokeState previousState = InvokeState.getCurrentState();
        InvokeState currentState = new InvokeState();

        User user = UserHelper.administrator();
        currentState.setSession(SessionHelper.create(currentThread.getName(), user));
        currentState.setUser(user);
        currentState.setCheckAccess(false);

        InvokeState.setCurrentState(currentState);
        InvokeState.setCurrentSession(currentState.getSession());
        InvokeState.setCurrentUser(currentState.getUser());

        try {
            if (BizDocEnvelopeHelper.setStatus(bizdoc, null, null, "ROUTING", "DEFERRED", false)) {
                // status was able to be changed, so we have a "lock" on the bizdoc and can now route it
                RoutingRuleHelper.route(rule, bizdoc, parameters);
            }
        } catch(ServiceException ex) {
            throw new RuntimeException(ex);
        } finally {
            StateManager.deleteContext(currentState.getSession().getSessionID());
            InvokeState.setCurrentState(previousState);
            currentThread.setName(originalThreadName);
        }
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
            cursor.insertAfter("bizdoc", bizdoc);
            cursor.insertAfter("rule", rule);
            if (parameters != null) cursor.insertAfter("TN_parms", parameters);
        } finally {
            cursor.destroy();
        }

        return document;
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