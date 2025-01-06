package net.snowflake.ingest.streaming.internal;

import java.util.concurrent.atomic.AtomicLong;

public class ObservabilityChannelState implements ChannelRuntimeState {
    private volatile boolean isValid;
    private volatile String endOffsetToken;
    private volatile String startOffsetToken;
    private final AtomicLong rowSequencer;

    public ObservabilityChannelState(String endOffsetToken, long rowSequencer, boolean isValid) {
        this.isValid = isValid;
        this.endOffsetToken = endOffsetToken;
        this.rowSequencer = new AtomicLong(rowSequencer);
    }

    @Override
    public boolean isValid() {
        return isValid;
    }

    @Override
    public String getEndOffsetToken() {
        return endOffsetToken;
    }

    @Override
    public String getStartOffsetToken() {
        return startOffsetToken;
    }

    @Override
    public long getRowSequencer() {
        return rowSequencer.get();
    }

    public void setValid(boolean valid) {
        isValid = valid;
    }

    public void setEndOffsetToken(String endOffsetToken) {
        this.endOffsetToken = endOffsetToken;
    }

    public void setStartOffsetToken(String startOffsetToken) {
        this.startOffsetToken = startOffsetToken;
    }

    public void setRowSequencer(long rowSequencer) {
        this.rowSequencer.set(rowSequencer);
    }
}
