package net.snowflake.ingest.streaming.example;

import java.util.Arrays;
import java.util.Collection;
import net.snowflake.ingest.utils.Constants;

public class ParquetPerf {

  public static Collection<Object[]> getParameters() {
    return Arrays.asList(
        new Object[][] {
          // 1M X 1
          {"Arrow", false, Constants.BdecVersion.ONE, 10000, 100, 1},
          {"Parquet", false, Constants.BdecVersion.THREE, 10000, 100, 1},
          {"Parquet", true, Constants.BdecVersion.THREE, 10000, 100, 1},
          // 1M X 2
          {"Arrow", false, Constants.BdecVersion.ONE, 10000, 100, 2},
          {"Parquet", false, Constants.BdecVersion.THREE, 10000, 100, 2},
          {"Parquet", true, Constants.BdecVersion.THREE, 10000, 100, 2},
          // 1M X 2
          {"Arrow", false, Constants.BdecVersion.ONE, 10000, 100, 3},
          {"Parquet", false, Constants.BdecVersion.THREE, 10000, 100, 3},
          {"Parquet", true, Constants.BdecVersion.THREE, 10000, 100, 3},
        });
  }

  public static void main(String[] args) {
    Collection<Object[]> param = getParameters();

    boolean[] nullables = {true, false};
    for (boolean n : nullables) {
      for (Object[] p : param) {
        SnowflakeStreamingIngestParquetPerfRunner perfRunner =
            new SnowflakeStreamingIngestParquetPerfRunner(
                (String) p[0],
                (Boolean) p[1],
                (Constants.BdecVersion) p[2],
                (Integer) p[3],
                (Integer) p[4],
                (Integer) p[5],
                n);
        try {
          perfRunner.setup();
          perfRunner.runPerfExperiment();
          perfRunner.tearDown();
          System.out.println("Run with " + Arrays.toString(p));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
