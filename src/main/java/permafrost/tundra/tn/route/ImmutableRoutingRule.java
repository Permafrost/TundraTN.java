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

import com.wm.app.tn.doc.BizDocAttribute;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.app.tn.route.PreRoutingFlags;
import com.wm.app.tn.route.RoutingRule;
import com.wm.data.IData;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.tn.util.TNFixedDataHelper;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Hashtable;

/**
 * A RoutingRule wrapper that makes the rule immutable.
 */
public class ImmutableRoutingRule extends RoutingRule {
    /**
     * The routing rule this rule wraps.
     */
    protected RoutingRule rule;

    /**
     * Creates a new ImmutableRoutingRule.
     *
     * @param rule  The rule to wrap.
     */
    public ImmutableRoutingRule(RoutingRule rule) {
        if (rule == null) throw new NullPointerException("rule must not be null");
        this.rule = rule;
    }

    @Override
    public void setID(String n) {
        // do nothing
    }

    @Override
    public String getID() {
        return rule.getID();
    }

    @Override
    public void setName(String n) {
        // do nothing
    }

    @Override
    public String getName() {
        return rule.getName();
    }

    @Override
    public void setDescription(String d) {
        // do nothing
    }

    @Override
    public String getDescription() {
        return rule.getDescription();
    }

    @Override
    public void setDisabled(boolean b) {
        // do nothing
    }

    @Override
    public boolean isDisabled() {
        return rule.isDisabled();
    }

    @Override
    public void setLastModifiedTime(Timestamp t) {
        // do nothing
    }

    @Override
    public Timestamp getLastModifiedTime() {
        return rule.getLastModifiedTime();
    }

    @Override
    public void setIndex(int i) {
        // do nothing
    }

    @Override
    public int getIndex() {
        return rule.getIndex();
    }

    @Override
    public void setSender(String[] sndId) {
        // do nothing
    }

    @Override
    public void setReceiver(String[] rcvId) {
        // do nothing
    }

    @Override
    public void setSenderGroups(String[] sndGrps) {
        // do nothing
    }

    @Override
    public void setReceiverGroups(String[] rcvGrps) {
        // do nothing
    }

    @Override
    public void setMessageType(String[] typeId) {
        // do nothing
    }

    @Override
    public void setUserStatus(String[] stat) {
        // do nothing
    }

    @Override
    public void containsErrors(String s) {
        // do nothing
    }

    @Override
    public void setSenderDisplayName(String[] sndName) {
        // do nothing
    }

    @Override
    public void setReceiverDisplayName(String[] rcvName) {
        // do nothing
    }

    @Override
    public void setMessageTypeName(String[] typeName) {
        // do nothing
    }

    @Override
    public void setSenderGroupsDisplayName(String[] sndGrpName) {
        // do nothing
    }

    @Override
    public void setReceiverGroupsDisplayName(String[] rcvGrpName) {
        // do nothing
    }

    @Override
    public void setToMwSString(String toMwSString) {
        // do nothing
    }

    @Override
    public String[] getSender() {
        String[] sender = rule.getSender();
        if (sender != null) {
            sender = Arrays.copyOf(sender, sender.length);
        }
        return sender;
    }

    @Override
    public String[] getReceiver() {
        String[] receiver = rule.getReceiver();
        if (receiver != null) {
            receiver = Arrays.copyOf(receiver, receiver.length);
        }
        return receiver;
    }

    @Override
    public String[] getSenderGroups() {
        String[] senderGroups = rule.getSenderGroups();
        if (senderGroups != null) {
            senderGroups = Arrays.copyOf(senderGroups, senderGroups.length);
        }
        return senderGroups;
    }

    @Override
    public String[] getReceiverGroups() {
        String[] receiverGroups = rule.getReceiverGroups();
        if (receiverGroups != null) {
            receiverGroups = Arrays.copyOf(receiverGroups, receiverGroups.length);
        }
        return receiverGroups;
    }

    @Override
    public String[] getMessageType() {
        String[] messageType = rule.getMessageType();
        if (messageType != null) {
            messageType = Arrays.copyOf(messageType, messageType.length);
        }
        return messageType;
    }

    @Override
    public String[] getUserStatus() {
        String[] userStatus = rule.getUserStatus();
        if (userStatus != null) {
            userStatus = Arrays.copyOf(userStatus, userStatus.length);
        }
        return userStatus;
    }

    @Override
    public String containsErrors() {
        return rule.containsErrors();
    }

    @Override
    public String[] getSenderDisplayName() {
        String[] senderDisplayName = rule.getSenderDisplayName();
        if (senderDisplayName != null) {
            senderDisplayName = Arrays.copyOf(senderDisplayName, senderDisplayName.length);
        }
        return senderDisplayName;
    }

    @Override
    public String[] getReceiverDisplayName() {
        String[] receiverDisplayName = rule.getReceiverDisplayName();
        if (receiverDisplayName != null) {
            receiverDisplayName = Arrays.copyOf(receiverDisplayName, receiverDisplayName.length);
        }
        return receiverDisplayName;
    }

    @Override
    public String[] getMessageTypeName() {
        String[] messageTypeName = rule.getMessageTypeName();
        if (messageTypeName != null) {
            messageTypeName = Arrays.copyOf(messageTypeName, messageTypeName.length);
        }
        return messageTypeName;
    }

    @Override
    public String[] getSenderGroupsDisplayName() {
        String[] senderGroupsDisplayName = rule.getSenderGroupsDisplayName();
        if (senderGroupsDisplayName != null) {
            senderGroupsDisplayName = Arrays.copyOf(senderGroupsDisplayName, senderGroupsDisplayName.length);
        }
        return senderGroupsDisplayName;
    }

    @Override
    public String[] getReceiverGroupsDisplayName() {
        String[] receiverDisplayName = rule.getReceiverDisplayName();
        if (receiverDisplayName != null) {
            receiverDisplayName = Arrays.copyOf(receiverDisplayName, receiverDisplayName.length);
        }
        return receiverDisplayName;
    }

    @Override
    public String getToMwSString() {
        return rule.getToMwSString();
    }

    @Override
    public void setAttribConditions(BizDocAttribute[] attr, String[][] cond) throws NumberFormatException, IllegalArgumentException {
        // do nothing
    }

    @Override
    public void setAttribConditions(String[][] cond) {
        // do nothing
    }

    @Override
    public String[][] getAttribConditions() {
        String[][] conditions = rule.getAttribConditions();

        if (conditions != null) {
            String[][] output = new String[conditions.length][];
            for (int i = 0; i < conditions.length; i++) {
                if (conditions[i] != null) {
                    output[i] = Arrays.copyOf(conditions[i], conditions[i].length);
                }
            }

            conditions = output;
        }

        return conditions;
    }

    @Override
    public boolean matches(String me, BizDocEnvelope d) {
        return rule.matches(me, d);
    }

    @Override
    public boolean matches(String me, BizDocEnvelope d, Hashtable atts) {
        return rule.matches(me, d, atts);
    }

    @Override
    public void setVerify(String s) {
        // do nothing
    }

    @Override
    public void setValidate(String s) {
        // do nothing
    }

    @Override
    public void setPersist(String s) {
        // do nothing
    }

    @Override
    public void setUniqueOption(String s) {
        // do nothing
    }

    @Override
    public void setPersistOption(String s) {
        // do nothing
    }

    @Override
    public PreRoutingFlags getPreRoutingFlags() {
        PreRoutingFlags flags;
        if (rule == null) {
            flags = new PreRoutingFlags("don't care", "don't care", "don't care", "don't care", "don't care");
        } else {
            flags = TNFixedDataHelper.duplicate(rule.getPreRoutingFlags());
        }
        return flags;
    }

    @Override
    public void setPreRoutingFlags(PreRoutingFlags flags) {
        // do nothing
    }

    @Override
    public void setAlert(String p_id, String p_type, String subject, String msg) {
        // do nothing
    }

    @Override
    public void setResponseMessage(String msg) {
        // do nothing
    }

    @Override
    public void setResponseMessage(String type, String msg) {
        // do nothing
    }

    @Override
    public void setIntendedUserStatus(String stat) {
        // do nothing
    }

    @Override
    public void setService(String name, IData input, String type) {
        // do nothing
    }

    @Override
    public void setSendTo(String proto) {
        // do nothing
    }

    @Override
    public void setDeliveryQueue(String queue) {
        // do nothing
    }

    @Override
    public String getAlertPartner() {
        return rule.getAlertPartner();
    }

    @Override
    public String getAlertContactType() {
        return rule.getAlertContactType();
    }

    @Override
    public String getAlertSubject() {
        return rule.getAlertSubject();
    }

    @Override
    public String getAlertMessage() {
        return rule.getAlertMessage();
    }

    @Override
    public String[] getResponseMessage() {
        String[] response = rule.getResponseMessage();
        if (response != null) {
            response = Arrays.copyOf(response, response.length);
        }
        return response;
    }

    @Override
    public String getIntendedUserStatus() {
        return rule.getIntendedUserStatus();
    }

    @Override
    public String getServiceName() {
        return rule.getServiceName();
    }

    @Override
    public IData getServiceInput() {
        return IDataHelper.duplicate(rule.getServiceInput(), true);
    }

    @Override
    public String getSendTo() {
        return rule.getSendTo();
    }

    @Override
    public String getDeliveryQueue() {
        return rule.getDeliveryQueue();
    }

    @Override
    public String getServiceInvokeType() {
        return rule.getServiceInvokeType();
    }

    @Override
    public void setLastChangeID(String lastChangeId) {
        // do nothing
    }

    @Override
    public String getLastChangeID() {
        return rule.getLastChangeID();
    }

    @Override
    public void setLastChangeUser(String lastChangeUser) {
        // do nothing
    }

    @Override
    public String getLastChangeUser() {
        return rule.getLastChangeUser();
    }

    @Override
    public void setLastChangeSession(String lastChangeSession) {
        // do nothing
    }

    @Override
    public String getLastChangeSession() {
        return rule.getLastChangeSession();
    }

    @Override
    public String toString() {
        return rule.toString();
    }

    @Override
    public String pipelineDataToString(IData idata, int i) {
        return rule.pipelineDataToString(idata, i);
    }
}
