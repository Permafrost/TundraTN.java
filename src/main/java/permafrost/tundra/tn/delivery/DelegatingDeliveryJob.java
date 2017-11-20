package permafrost.tundra.tn.delivery;

import com.wm.app.tn.delivery.DeliveryJob;
import com.wm.app.tn.delivery.DeliveryService;
import com.wm.app.tn.doc.BizDocEnvelope;
import com.wm.data.IData;

/**
 * Provides a proxy or delegating wrapper for a DeliveryJob.
 */
public class DelegatingDeliveryJob extends DeliveryJob {
    /**
     * The delegate all method calls are delegated to.
     */
    protected DeliveryJob delegate;

    /**
     * Create a new DelegatingDeliveryJob.
     *
     * @param delegate The delegate all method calls are delegated to.
     */
    public DelegatingDeliveryJob(DeliveryJob delegate) {
        this.delegate = delegate;
    }

    /**
     * @return The delegate this object delegates method calls to.
     */
    public DeliveryJob getDelegate() {
        return delegate;
    }

    @Override
    public void addActivityEntry(int type, String brief, String full) {
        delegate.addActivityEntry(type, brief, full);
    }

    @Override
    public boolean attempted() {
        return delegate.attempted();
    }

    @Override
    public void delivering() {
        delegate.delivering();
    }

    @Override
    public void done() {
        delegate.done();
    }

    @Override
    public void enqueue(String queue) {
        delegate.enqueue(queue);
    }

    @Override
    public boolean exceedsRetries() {
        return delegate.exceedsRetries();
    }

    @Override
    public void fail() {
        delegate.fail();
    }

    @Override
    public BizDocEnvelope getBizDocEnvelope() {
        return delegate.getBizDocEnvelope();
    }

    @Override
    public String getClassification() {
        return delegate.getClassification();
    }

    @Override
    public IData getDBIData() {
        return delegate.getDBIData();
    }

    @Override
    public DeliveryService getDeliveryService() {
        return delegate.getDeliveryService();
    }

    @Override
    public IData getInputData() {
        return delegate.getInputData();
    }

    @Override
    public String getInvokeAsUser() {
        return delegate.getInvokeAsUser();
    }

    @Override
    public String getJobId() {
        return delegate.getJobId();
    }

    @Override
    public IData getOutputData() {
        return delegate.getOutputData();
    }

    @Override
    public String getQueueName() {
        return delegate.getQueueName();
    }

    @Override
    public int getRetries() {
        return delegate.getRetries();
    }

    @Override
    public int getRetryFactor() {
        return delegate.getRetryFactor();
    }

    @Override
    public int getRetryLimit() {
        return delegate.getRetryLimit();
    }

    @Override
    public String getServerId() {
        return delegate.getServerId();
    }

    @Override
    public DeliveryService getService() {
        return delegate.getService();
    }

    @Override
    public String getStatus() {
        return delegate.getStatus();
    }

    @Override
    public String getStatusMsg() {
        return delegate.getStatusMsg();
    }

    @Override
    public int getStatusVal() {
        return delegate.getStatusVal();
    }

    @Override
    public long getTTW() {
        return delegate.getTTW();
    }

    @Override
    public long getTimeCreated() {
        return delegate.getTimeCreated();
    }

    @Override
    public long getTimeUpdated() {
        return delegate.getTimeUpdated();
    }

    @Override
    public String getTransportStatus() {
        return delegate.getTransportStatus();
    }

    @Override
    public String getTransportStatusMessage() {
        return delegate.getTransportStatusMessage();
    }

    @Override
    public long getTransportTime() {
        return delegate.getTransportTime();
    }

    @Override
    public void hold() {
        delegate.hold();
    }

    @Override
    public void invoke() {
        delegate.invoke();
    }

    @Override
    public boolean isActive() {
        return delegate.isActive();
    }

    @Override
    public boolean isComplete() {
        return delegate.isComplete();
    }

    @Override
    public boolean isDelivering() {
        return delegate.isDelivering();
    }

    @Override
    public boolean isDone() {
        return delegate.isDone();
    }

    @Override
    public boolean isExpired() {
        return delegate.isExpired();
    }

    @Override
    public boolean isFailed() {
        return delegate.isFailed();
    }

    @Override
    public boolean isHeld() {
        return delegate.isHeld();
    }

    @Override
    public boolean isNew() {
        return delegate.isNew();
    }

    @Override
    public boolean isPending() {
        return delegate.isPending();
    }

    @Override
    public boolean isProcessing() {
        return delegate.isProcessing();
    }

    @Override
    public boolean isQueued() {
        return delegate.isQueued();
    }

    @Override
    public boolean isRetry() {
        return delegate.isRetry();
    }

    @Override
    public boolean isStopped() {
        return delegate.isStopped();
    }

    @Override
    public boolean readyToRun() {
        return delegate.readyToRun();
    }

    @Override
    public void reset() {
        delegate.reset();
    }

    @Override
    public boolean retriesReached() {
        return delegate.retriesReached();
    }

    @Override
    public void retryFailed() {
        delegate.retryFailed();
    }

    @Override
    public boolean save() {
        return delegate.save();
    }

    @Override
    public void setActive(boolean active) {
        delegate.setActive(active);
    }

    @Override
    public void setAttempted(boolean attempted) {
        delegate.setAttempted(attempted);
    }

    @Override
    public void setBizDocEnvelope(BizDocEnvelope bizdoc) {
        delegate.setBizDocEnvelope(bizdoc);
    }

    @Override
    public void setDBIData(IData data) {
        delegate.setDBIData(data);
    }

    @Override
    public void setDefaultServerId() {
        delegate.setDefaultServerId();
    }

    @Override
    public void setInputData(IData inputData) {
        delegate.setInputData(inputData);
    }

    @Override
    public void setInvokeAsUser(String invokeAsUser) {
        delegate.setInvokeAsUser(invokeAsUser);
    }

    @Override
    public void setJobId(String id) {
        delegate.setJobId(id);
    }

    @Override
    public void setOutputData(IData outputData) {
        delegate.setOutputData(outputData);
    }

    @Override
    public void setProcessing(boolean processing) {
        delegate.setProcessing(processing);
    }

    @Override
    public void setQueueName(String queue) {
        delegate.setQueueName(queue);
    }

    @Override
    public void setRetries(int retries) {
        delegate.setRetries(retries);
    }

    @Override
    public void setRetryFactor(int retryFactor) {
        delegate.setRetryFactor(retryFactor);
    }

    @Override
    public void setRetryLimit(int retryLimit) {
        delegate.setRetryLimit(retryLimit);
    }

    @Override
    public void setServerId(String id) {
        delegate.setServerId(id);
    }

    @Override
    public void setService(DeliveryService service) {
        delegate.setService(service);
    }

    @Override
    public void setStatus(int status) {
        delegate.setStatus(status);
    }

    @Override
    public void setTTW(long ttw) {
        delegate.setTTW(ttw);
    }

    @Override
    public void setTimeCreated(long time) {
        delegate.setTimeCreated(time);
    }

    @Override
    public void setTimeUpdated(long time) {
        delegate.setTimeUpdated(time);
    }

    @Override
    public void setTransportStatus(String status) {
        delegate.setTransportStatus(status);
    }

    @Override
    public void setTransportStatusMessage(String message) {
        delegate.setTransportStatusMessage(message);
    }

    @Override
    public void setTransportTime(long time) {
        delegate.setTransportTime(time);
    }

    @Override
    public void start() {
        delegate.start();
    }

    @Override
    public void stop() {
        delegate.stop();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
