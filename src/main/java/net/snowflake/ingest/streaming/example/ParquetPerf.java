package net.snowflake.ingest.streaming.example;

import java.util.Arrays;
import java.util.Collection;

import net.snowflake.ingest.utils.Constants;

public class ParquetPerf {

    private static int n_10_k = 10 * 1000;
    private static int n_100_k = 100 * 1000;
    private static int n_1_M = 1000 * 1000;
    private static int numRuns = 4;

    public static Collection<Object[]> getParameters() {
        return Arrays.asList(
                new Object[][]{
                        // 100k x 10
                        {"Arrow", false, Constants.BdecVersion.ONE, n_10_k, 10, 10},
                        {"Parquet", false, Constants.BdecVersion.THREE, n_10_k, 10, 10},
                        // 10k x 100
                        {"Arrow", false, Constants.BdecVersion.ONE, n_10_k, 1, 100},
                        {"Parquet", false, Constants.BdecVersion.THREE, n_10_k, 1, 100},
                        // 1M X 1
                        {"Arrow", false, Constants.BdecVersion.ONE, n_10_k, 100, 1},
                        {"Parquet", false, Constants.BdecVersion.THREE, n_10_k, 100, 1},
                        // 1M X 2
                        {"Arrow", false, Constants.BdecVersion.ONE, n_10_k, 100, 2},
                        {"Parquet", false, Constants.BdecVersion.THREE, n_10_k, 100, 2},
                        // 1M x 3
                        {"Arrow", false, Constants.BdecVersion.ONE, n_10_k, 100, 3},
                        {"Parquet", false, Constants.BdecVersion.THREE, n_10_k, 100, 3},
                });
    }

    public static void main(String[] args) {
        Collection<Object[]> param = getParameters();

        boolean[] nullables = {false};
        for (boolean n : nullables) {
            for (Object[] p : param) {
                for (int i = 0; i < numRuns; i++) {
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
}
