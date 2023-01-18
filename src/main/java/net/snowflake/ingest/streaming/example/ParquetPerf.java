package net.snowflake.ingest.streaming.example;

import java.util.Arrays;
import java.util.Collection;
import net.snowflake.ingest.utils.Constants;

public class ParquetPerf {

  public static Collection<Object[]> getParameters() {
    return Arrays.asList(
        new Object[][] {
          // 100k X 100
          {"Arrow", false, Constants.BdecVersion.ONE, 1000, 100, 10},
          {"Parquet", false, Constants.BdecVersion.THREE, 1000, 100, 10},
          {"Parquet", true, Constants.BdecVersion.THREE, 1000, 100, 10},
          // 1M X 1
          {"Arrow", false, Constants.BdecVersion.ONE, 10000, 100, 1},
          {"Parquet", false, Constants.BdecVersion.THREE, 10000, 100, 1},
          {"Parquet", true, Constants.BdecVersion.THREE, 10000, 100, 1},
          // 10k X 100
          {"Arrow", false, Constants.BdecVersion.ONE, 1000, 10, 100},
          {"Parquet", false, Constants.BdecVersion.THREE, 1000, 10, 100},
          {"Parquet", true, Constants.BdecVersion.THREE, 1000, 10, 100},
          // 100k X 36
          {"Arrow", false, Constants.BdecVersion.ONE, 1000, 100, 36},
          {"Parquet", false, Constants.BdecVersion.THREE, 1000, 100, 36},
          {"Parquet", true, Constants.BdecVersion.THREE, 1000, 100, 36},
          // 100k X 1
          {"Arrow", false, Constants.BdecVersion.ONE, 1000, 100, 1},
          {"Parquet", false, Constants.BdecVersion.THREE, 1000, 100, 1},
          {"Parquet", true, Constants.BdecVersion.THREE, 1000, 100, 1},
          // 100k X 2
          {"Arrow", false, Constants.BdecVersion.ONE, 1000, 100, 2},
          {"Parquet", false, Constants.BdecVersion.THREE, 1000, 100, 2},
          {"Parquet", true, Constants.BdecVersion.THREE, 1000, 100, 2},
          // 100k X 3
          {"Arrow", false, Constants.BdecVersion.ONE, 1000, 100, 3},
          {"Parquet", false, Constants.BdecVersion.THREE, 1000, 100, 3},
          {"Parquet", true, Constants.BdecVersion.THREE, 1000, 100, 3},
          // 100k X 4
          {"Arrow", false, Constants.BdecVersion.ONE, 1000, 100, 4},
          {"Parquet", false, Constants.BdecVersion.THREE, 1000, 100, 4},
          {"Parquet", true, Constants.BdecVersion.THREE, 1000, 100, 4},
          // 100k X 5
          {"Arrow", false, Constants.BdecVersion.ONE, 1000, 100, 5},
          {"Parquet", false, Constants.BdecVersion.THREE, 1000, 100, 5},
          {"Parquet", true, Constants.BdecVersion.THREE, 1000, 100, 5}
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
