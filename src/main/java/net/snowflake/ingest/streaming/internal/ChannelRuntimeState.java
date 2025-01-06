package net.snowflake.ingest.streaming.internal;

interface ChannelRuntimeState {
    boolean isValid();

    String getEndOffsetToken();

    String getStartOffsetToken();

    long getRowSequencer();
}
