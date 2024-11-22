/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.it;

import org.junit.Before;
import org.junit.runners.Parameterized;

public class FDNBigFilesIT extends BigFilesITBase {
  @Parameterized.Parameters(name = "compressionAlgorithm={0}")
  public static Object[][] compressionAlgorithms() {
    return new Object[][] {{"GZIP", null}, {"ZSTD", null}};
  }

  @Before
  public void before() throws Exception {
    super.beforeAll(false);
  }
}
