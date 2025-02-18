/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import org.junit.Before;

public class FDNOpenManyChannelsIT extends OpenManyChannelsITBase {
  @Before
  public void before() throws Exception {
    super.setUp(false);
  }
}
