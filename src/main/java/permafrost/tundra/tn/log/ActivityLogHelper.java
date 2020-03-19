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

package permafrost.tundra.tn.log;

import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.app.tn.err.ActivityLogEntry;
import com.wm.app.tn.err.SystemLog2;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.lang.ns.NSService;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.data.IDataYAMLParser;
import permafrost.tundra.data.transform.Transformer;
import permafrost.tundra.data.transform.string.Squeezer;
import permafrost.tundra.lang.IterableHelper;
import permafrost.tundra.lang.StringHelper;
import permafrost.tundra.server.NameHelper;
import permafrost.tundra.server.ServiceHelper;
import permafrost.tundra.time.DateTimeHelper;
import permafrost.tundra.time.DurationHelper;
import permafrost.tundra.time.DurationPattern;
import permafrost.tundra.tn.document.BizDocEnvelopeHelper;
import java.io.IOException;
import java.util.List;

/**
 * A collection of convenience methods for working with the Trading Networks Activity Log.
 */
public class ActivityLogHelper {
    /**
     * Disallow instantiation of this class.
     */
    private ActivityLogHelper() {}

    /**
     * Logs the given message in the Trading Network's Activity Log.
     *
     * @param entryType      The entry type of the log.
     * @param entryClass     The entry class of the log.
     * @param messageSummary The summary of the message being logged.
     * @param messageDetail  The detail of the message being logged.
     * @param bizdoc         Optional related bizdoc to log against.
     */
    public static void log(EntryType entryType, String entryClass, String messageSummary, String messageDetail, BizDocEnvelope bizdoc) {
        log(entryType, entryClass, messageSummary, messageDetail, bizdoc, null);
    }

    /**
     * Logs the given message in the Trading Network's Activity Log.
     *
     * @param entryType      The entry type of the log.
     * @param entryClass     The entry class of the log.
     * @param messageSummary The summary of the message being logged.
     * @param messageDetail  The detail of the message being logged.
     * @param bizdoc         Optional related bizdoc to log against.
     * @param startTime      The start time in nanoseconds.
     * @param endTime        The end time in nanoseconds.
     */
    public static void log(EntryType entryType, String entryClass, String messageSummary, String messageDetail, BizDocEnvelope bizdoc, long startTime, long endTime) {
        log(entryType, entryClass, messageSummary, messageDetail, bizdoc, getContext(null, startTime, endTime));
    }

    /**
     * Logs the given message in the Trading Network's Activity Log.
     *
     * @param entryType      The entry type of the log.
     * @param entryClass     The entry class of the log.
     * @param messageSummary The summary of the message being logged.
     * @param messageDetail  The detail of the message being logged.
     * @param bizdoc         Optional related bizdoc to log against.
     * @param duration       The duration in seconds to be included in the log context.
     */
    public static void log(EntryType entryType, String entryClass, String messageSummary, String messageDetail, BizDocEnvelope bizdoc, double duration) {
        log(entryType, entryClass, messageSummary, messageDetail, bizdoc, getContext(null, duration));
    }

    /**
     * Returns a context decorated with the given duration.
     *
     * @param context   The context to decorate.
     * @param startTime The start time in nanoseconds.
     * @param endTime   The end time in nanoseconds.
     * @return          The context decorated with the given duration.
     */
    public static IData getContext(IData context, long startTime, long endTime) {
        return getContext(context, (endTime - startTime) / 1000000000.0);
    }

    /**
     * Returns a context decorated with the given duration.
     *
     * @param context   The context to decorate.
     * @param duration  The duration in seconds.
     * @return          The context decorated with the given duration.
     */
    public static IData getContext(IData context, double duration) {
        if (context == null) context = IDataFactory.create();
        IDataHelper.put(context, "Duration", DurationHelper.format(duration, DurationPattern.XML_NANOSECONDS));
        return context;
    }

    /**
     * Logs the given message in the Trading Network's Activity Log.
     *
     * @param entryType      The entry type of the log.
     * @param entryClass     The entry class of the log.
     * @param messageSummary The summary of the message being logged.
     * @param messageDetail  The detail of the message being logged.
     * @param bizdoc         Optional related bizdoc to log against.
     * @param context        Optional document containing key values to be included in the log for context.
     */
    @SuppressWarnings("deprecation")
    public static void log(EntryType entryType, String entryClass, String messageSummary, String messageDetail, BizDocEnvelope bizdoc, IData context) {
        if (bizdoc == null || BizDocEnvelopeHelper.shouldPersistActivityLog(bizdoc)) {
            entryType = EntryType.normalize(entryType);

            if (messageDetail == null) messageDetail = "";
            String[] messageDetailLines = StringHelper.lines(messageDetail);
            if (messageSummary == null) messageSummary = messageDetailLines[0];
            if (entryClass == null) entryClass = "General";

            StringBuilder messageDetailBuilder = new StringBuilder();
            for (String line : messageDetailLines) {
                messageDetailBuilder.append(line);
                messageDetailBuilder.append("\n");
            }
            messageDetailBuilder.append("---\n");
            messageDetailBuilder.append(getContextString(context));

            messageSummary = StringHelper.truncate(messageSummary.trim(), 240, true);
            messageDetail = StringHelper.truncate(messageDetailBuilder.toString().trim(), 1024, true);

            ActivityLogEntry log = new ActivityLogEntry(entryType.getValue(), entryClass, messageSummary, messageDetail);
            if (bizdoc != null) {
                log.setRelatedDocId(bizdoc.getInternalId());
                String conversationID = bizdoc.getConversationId();
                if (conversationID != null) log.setRelatedConversationId(conversationID);
                String partnerID = bizdoc.getSenderId();
                if (partnerID != null) log.setRelatedPartnerId(partnerID);
            }
            SystemLog2.dbLog(log);
        }
    }

    /**
     * Decorates the given context with additional useful data such as the host name and time, then returns the given
     * context as a string.
     *
     * @param context   The context to convert to a string.
     * @return          A string representing the given context.
     */
    private static String getContextString(IData context) {
        String output = "";

        if (context == null) {
            context = IDataFactory.create();
        } else {
            context = IDataHelper.duplicate(context, true);
        }

        IDataCursor contextCursor = context.getCursor();
        try {
            IDataHelper.put(contextCursor, "Host", IDataHelper.get(NameHelper.localhost(), "$host"), false);
            IDataHelper.put(contextCursor, "Time", DateTimeHelper.now("datetime"), false);
            IDataHelper.put(contextCursor, "Session", Service.getSession().getSessionID());
            Thread currentThread = Thread.currentThread();
            IDataHelper.put(contextCursor, "Thread", currentThread.getClass().getName() + "#" + currentThread.getId() + " " + currentThread.getName());

            List<NSService> callStack = ServiceHelper.getCallStack();
            if (callStack.size() > 0) {
                if (callStack.size() > 1) {
                    int index = callStack.size() - 1;
                    if ("tundra.tn:log".equals(callStack.get(index).toString())) {
                        callStack.remove(index);
                    }
                }
                IDataHelper.put(contextCursor, "Service", IterableHelper.join(callStack, " → "));
            }

            context = Transformer.transform(context, new Squeezer(true));
            IDataYAMLParser parser = new IDataYAMLParser();
            output = parser.emit(context, String.class);
        } catch (IOException ex) {
            // ignore exception
        } catch (ServiceException ex) {
            // ignore exception
        } finally {
            contextCursor.destroy();
        }

        return output;
    }
}
