/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.it;

import org.junit.Before;

public class FdnColumnNamesIT extends ColumnNamesITBase {
  @Before
  public void before() throws Exception {
    super.setUp(false, "GZIP", null);
  }
}
