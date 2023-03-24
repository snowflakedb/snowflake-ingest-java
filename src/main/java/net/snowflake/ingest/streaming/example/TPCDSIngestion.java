/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.example;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.streaming.internal.SnowflakeStreamingIngestClientInternal;
import net.snowflake.ingest.utils.Constants;

/**
 * Example on how to use the Streaming Ingest client APIs.
 *
 * <p>Please read the README.md file for detailed steps
 */
public class TPCDSIngestion {
    private static String[] tables = new String[]{"call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer", "customer_address", "customer_demographics", "date_dim", "dbgen_version", "household_demographics",
            "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site"};

    //private static String[] tables = new String[]{"call_center", "catalog_page", "catalog_returns"};
    private static final String originalDatabase = "benchmark_db";
    private static final String originalSchema = "tpcds_sf1";

    private static int batchSize = 10000;

    public static void main(String[] args) throws Exception {
        Properties props = Util.getProperties(Constants.BdecVersion.THREE);


        int cores = Runtime.getRuntime().availableProcessors();

        // Create a streaming ingest client
        SnowflakeStreamingIngestClientInternal client =
                (SnowflakeStreamingIngestClientInternal<?>)
                        SnowflakeStreamingIngestClientFactory.builder("gdoci")
                                .setProperties(props)
                                .build();


            int numTables = tables.length; // 1 channel per table

            List<SnowflakeStreamingIngestChannel> channelList = new ArrayList<>();
            for (int i = 0; i < numTables; i++) {
                final String channelName = "CHANNEL" + i;
                final String table = tables[i];
                SnowflakeStreamingIngestChannel channel = Util.openChannel(props.getProperty("database"), props.getProperty("schema"), table, channelName, client);
                channelList.add(channel);
                Connection jdbcConnection;
                ResultSet result;
                    // query table
                try {
                    jdbcConnection = Util.getConnection();
                    jdbcConnection
                            .createStatement()
                            .execute(String.format("use warehouse %s", Util.getWarehouse()));


                    result = jdbcConnection.createStatement().executeQuery(
                    String.format(
                            "select count(*) from %s.%s.%s",
                            originalDatabase, originalSchema, table));
                    result.next();
                    long originalCount = result.getLong(1);
                    result = jdbcConnection.createStatement().executeQuery(
                    String.format(
                            "select * from %s.%s.%s",
                            originalDatabase, originalSchema, table));
                    List rows = Util.resultSetToArrayListAndIngestInBatches(result, batchSize, channel, originalCount);

                    Util.verifyInsertValidationResponse(channel.insertRows(rows, String.valueOf(originalCount)));

                    Util.waitChannelFlushed(channel, (int) originalCount);
                    channel.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }
            client.close();

            // verify results
        for (int i = 0; i < numTables; i++) {
            final String table = tables[i];
            Connection jdbcConnection;
            ResultSet result;
            // query table
            try {
                jdbcConnection = Util.getConnection();
                jdbcConnection
                        .createStatement()
                        .execute(String.format("use warehouse %s", Util.getWarehouse()));


                result = jdbcConnection.createStatement().executeQuery(
                        String.format(
                                "select count(*) from %s.%s.%s",
                                originalDatabase, originalSchema, table));
                result.next();
                long originalCount = result.getLong(1);

                result = jdbcConnection.createStatement().executeQuery(
                        String.format(
                                "select count(*) from %s.%s.%s",
                                props.getProperty("database"), props.getProperty("schema"), table));
                result.next();
                long expCount = result.getLong(1);
                if (expCount != originalCount) {
                    System.err.println("expCount=" + expCount + ", originalCount=" + originalCount + " for table=" + table);
                } else {
                    System.out.println("Success for table=" + table);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }


}
