package permafrost.tundra.tn.document;

import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.lang.ThreadHelper;
import permafrost.tundra.tn.route.CallableRoute;
import permafrost.tundra.util.concurrent.Priority;
import permafrost.tundra.util.concurrent.SequencePriority;
import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

/**
 * Priority implementation for CallableRoute objects.
 */
public class BizDocEnvelopePriority extends SequencePriority {
    /**
     * The create datetime to the nearest minute for the bizdoc being routed.
     */
    protected long createDateTime;
    /**
     * The message priority used to prioritize bizdocs created in the same minute.
     */
    protected double messagePriority;
    /**
     * The thread priority to use when processing or routing the bizdoc.
     */
    protected int threadPriority = Thread.NORM_PRIORITY;

    /**
     * Construct a new BizDocEnvelopePriority.
     *
     * @param bizdoc    The bizdoc whose route is to be prioritized.
     */
    public BizDocEnvelopePriority(BizDocEnvelope bizdoc) {
        this(bizdoc, 60 * 1000);
    }

    /**
     * Construct a new BizDocEnvelopePriority.
     *
     * @param bizdoc    The bizdoc whose route is to be prioritized.
     * @param duration  The duration in milliseconds in which bizdocs are prioritized.
     */
    public BizDocEnvelopePriority(BizDocEnvelope bizdoc, long duration) {
        this(bizdoc, duration, TimeUnit.MILLISECONDS);
    }

    /**
     * Construct a new BizDocEnvelopePriority.
     *
     * @param bizdoc    The bizdoc whose route is to be prioritized.
     * @param duration  The duration in which bizdocs are prioritized.
     * @param unit      The time unit the duration is measured in.
     */
    public BizDocEnvelopePriority(BizDocEnvelope bizdoc, long duration, TimeUnit unit) {
        if (bizdoc == null) throw new NullPointerException("bizdoc must not be null");

        this.createDateTime = bizdoc.getTimestamp().getTime() / unit.toMillis(duration);
        this.messagePriority = CallableRoute.DEFAULT_MESSAGE_PRIORITY;

        IData attributes = bizdoc.getAttributes();
        if (attributes != null) {
            IDataCursor cursor = attributes.getCursor();
            try {
                BigDecimal messagePriorityAttribute = IDataHelper.get(cursor, CallableRoute.MESSAGE_PRIORITY_ATTRIBUTE_NAME, BigDecimal.class);
                if (messagePriorityAttribute != null) {
                    messagePriority = messagePriorityAttribute.doubleValue();
                }

                BigDecimal threadPriorityAttribute = IDataHelper.get(cursor, CallableRoute.THREAD_PRIORITY_ATTRIBUTE_NAME, BigDecimal.class);
                if (threadPriorityAttribute != null) {
                    threadPriority = ThreadHelper.normalizePriority(threadPriorityAttribute.intValue());
                }
            } finally {
                cursor.destroy();
            }
        }
    }

    /**
     * Returns the thread priority to use when routing.
     *
     * @return the thread priority to use when routing.
     */
    public int getThreadPriority() {
        return threadPriority;
    }

    /**
     * Compares this CallableRoutePriority with another CallableRoutePriority, in order to prioritize routes.
     *
     * @param other The other CallableRoutePriority to compare to.
     * @return      The result of the comparison.
     */
    @Override
    public int compareTo(Priority other) {
        int comparison;
        if (other instanceof BizDocEnvelopePriority) {
            long otherCreateDateTime = ((BizDocEnvelopePriority)other).createDateTime;
            double otherMessagePriority = ((BizDocEnvelopePriority)other).messagePriority;

            if (createDateTime < otherCreateDateTime) {
                comparison = -1;
            } else if (createDateTime > otherCreateDateTime) {
                comparison = 1;
            } else {
                if (messagePriority > otherMessagePriority) {
                    comparison = -1;
                } else if (messagePriority < otherMessagePriority) {
                    comparison = 1;
                } else {
                    comparison = super.compareTo(other);
                }
            }
        } else {
            // cannot compare with different priority implementation, so default to equal
            comparison = 0;
        }
        return comparison;
    }
}
