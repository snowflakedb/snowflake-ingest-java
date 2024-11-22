/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.it;

import org.junit.Before;

public class FDNManyTablesIT extends ManyTablesITBase {
  @Before
  public void before() throws Exception {
    super.setUp(false);
  }
}
