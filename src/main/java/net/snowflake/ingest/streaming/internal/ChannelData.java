/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.List;
import org.apache.arrow.vector.FieldVector;

/**
 * Contains the data and metadata returned for each channel flush, which will be used to build the
 * blob and register blob request
 */
class ChannelData {
  private Long rowSequencer;
  private String offsetToken;
  private List<FieldVector> vectors;
  private long rowCount;
  private float bufferSize;
  private SnowflakeStreamingIngestChannelInternal channel;
  private EpInfo epInfo;

  Long getRowSequencer() {
    return this.rowSequencer;
  }

  void setRowSequencer(Long rowSequencer) {
    this.rowSequencer = rowSequencer;
  }

  String getOffsetToken() {
    return this.offsetToken;
  }

  void setOffsetToken(String offsetToken) {
    this.offsetToken = offsetToken;
  }

  List<FieldVector> getVectors() {
    return this.vectors;
  }

  void setVectors(List<FieldVector> vectors) {
    this.vectors = vectors;
  }

  long getRowCount() {
    return this.rowCount;
  }

  void setRowCount(long rowCount) {
    this.rowCount = rowCount;
  }

  float getBufferSize() {
    return this.bufferSize;
  }

  void setBufferSize(float bufferSize) {
    this.bufferSize = bufferSize;
  }

  SnowflakeStreamingIngestChannelInternal getChannel() {
    return this.channel;
  }

  void setChannel(SnowflakeStreamingIngestChannelInternal channel) {
    this.channel = channel;
  }

  EpInfo getEpInfo() {
    return epInfo;
  }

  void setEpInfo(EpInfo epInfo) {
    this.epInfo = epInfo;
  }

  @Override
  public String toString() {
    return this.channel.toString();
  }
}
