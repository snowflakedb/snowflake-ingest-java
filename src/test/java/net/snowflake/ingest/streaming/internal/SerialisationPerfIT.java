package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.internal.AbstractRowBuffer.ColumnLogicalType;
import net.snowflake.ingest.streaming.internal.AbstractRowBuffer.ColumnPhysicalType;
import net.snowflake.ingest.streaming.internal.Flusher.SerializationResult;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.Constants.BdecVersion;
import net.snowflake.ingest.utils.Pair;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static net.snowflake.ingest.streaming.internal.BlobBuilder.compressIfNeededAndPadChunk;

@RunWith(Parameterized.class)
public class SerialisationPerfIT {
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> bdecVersion() {
        return Arrays.asList(
                new Object[][] {
                        {"Arrow", Constants.BdecVersion.ONE}, {"Parquet", Constants.BdecVersion.THREE}
                });
    }

    private final BdecVersion bdecVersion;

    public SerialisationPerfIT(
            @SuppressWarnings("unused") String name, BdecVersion bdecVersion) {
        this.bdecVersion = bdecVersion;
    }

    private static class BufferChannelContext<T> {
        final int index;
        final ChannelFlushContext channelFlushContext;
        final AbstractRowBuffer<T> buffer;
        float size = 0;

        private BufferChannelContext(BdecVersion bdecVersion, int index, List<ColumnMetadata> columns, RootAllocator allocator) {
            this.index = index;
            this.channelFlushContext = new ChannelFlushContext("SerialisationPerfITChannel" + index, "dummyDb", "dummySchema", "dummyTable", 0L, "encryptionKey", 1L);
            this.buffer = AbstractRowBuffer.createRowBuffer(
                    OpenChannelRequest.OnErrorOption.CONTINUE,
                    allocator,
                    bdecVersion,
                    "test.buffer" + index,
                    rs -> size += rs,
                    new ChannelRuntimeState("0", 0, true), false);
            buffer.setupSchema(columns);
        }
    }

    private static class FileStats {
        final long runtimeMilli;
        final float rowSize;
        final int fileSize;
        final int rowNumber;

        private FileStats(long runtimeMilli, float rowSize, int fileSize, int rowNumber) {
            this.runtimeMilli = runtimeMilli;
            this.rowSize = rowSize;
            this.fileSize = fileSize;
            this.rowNumber = rowNumber;
        }
    }

    //@Ignore
    @Test
    public void test() throws IOException {
        final int numberOfChannels = 10;
        final int rowNumber = 100000;

        List<ColumnMetadata> columns = createColumns(true);

        Map<String, RowBufferStats> statsMap = new HashMap<>();
        for (ColumnMetadata column : columns) {
            statsMap.put(column.getName(), new RowBufferStats(column.getName()));
        }

        List<FileStats> fileStatsList = run(numberOfChannels, rowNumber, columns, statsMap);
        long totalRuntimeMilli = fileStatsList.stream().mapToLong(s -> s.runtimeMilli).sum();
        double avgRuntimeMilli = fileStatsList.stream().mapToLong(s -> s.runtimeMilli).average().getAsDouble();
        double totalRowSize = fileStatsList.stream().mapToDouble(s -> (double) s.rowSize).sum();
        double avgRowSize = fileStatsList.stream().mapToDouble(s -> (double) s.rowSize).average().getAsDouble();
        long totalFileSize = fileStatsList.stream().mapToLong(s -> (long)s.fileSize).sum();
        double avgFileSize = fileStatsList.stream().mapToLong(s -> (long)s.fileSize).average().getAsDouble();
        long totalRowNumber = fileStatsList.stream().mapToLong(s -> (long)s.rowNumber).sum();
        double avgRowNumber = fileStatsList.stream().mapToLong(s -> (long)s.rowNumber).average().getAsDouble();

        System.out.printf(
                "fileNumber=%s\n" +
                "totalRuntimeMilli=%s, avgRuntimeMilli=%s\n" +
                    "totalRowSize=%s, avgRowSize=%s\n" +
                    "totalFileSize=%s, avgFileSize=%s\n" +
                        "totalRowNumber=%s, avgRowNumber=%s\n%n",
                fileStatsList.size(),
                totalRuntimeMilli,
                avgRuntimeMilli,
                totalRowSize,
                avgRowSize,
                totalFileSize,
                avgFileSize,
                totalRowNumber,
                avgRowNumber);

//        try (FileOutputStream outputStream = new FileOutputStream(filePath)) {
//            outputStream.write(chunkData.toByteArray());
//        }
    }

    private <T> List<FileStats> run(int numberOfChannels, int rowNumber, List<ColumnMetadata> columns, Map<String, RowBufferStats> statsMap) throws IOException {
        RootAllocator allocator = new RootAllocator();
        List<BufferChannelContext<T>> buffers = IntStream.range(0, numberOfChannels)
                .mapToObj(i -> new BufferChannelContext<T>(bdecVersion, i, columns, allocator))
                .collect(Collectors.toList());

        List<FileStats> fileStatsList = new ArrayList<>();
        Random r = new Random();
        int fileIndex = 0;
        int rows = 0;
        long fileStartTimeMilli = System.currentTimeMillis();
        for (int i = 0; i < rowNumber; i++) {
            for (BufferChannelContext<?> bufferChannelContext : buffers) {
                bufferChannelContext.buffer.insertRows(Collections.singletonList(getRandomRow(r)), null);
            }
            rows++;
            float size = (float) buffers.stream().mapToDouble(b -> b.size).sum();
            if (size >= Constants.MAX_CHUNK_SIZE_IN_BYTES) {
                int fileSize = flush(buffers, fileIndex + ".bdec");
                long runtimeMilli = System.currentTimeMillis() - fileStartTimeMilli;
                FileStats fileStats = new FileStats(runtimeMilli, size, fileSize, rows);
                fileStatsList.add(fileStats);
                for (BufferChannelContext<?> bufferChannelContext : buffers) {
                    bufferChannelContext.buffer.reset();
                    bufferChannelContext.size = 0;
                }
                size = 0;
                rows = 0;
                fileStartTimeMilli = System.currentTimeMillis();
                fileIndex++;
            }
        }
        float size = (float) buffers.stream().mapToDouble(b -> b.size).sum();
        int fileSize = flush(buffers, fileIndex + ".bdec");
        long runtimeMilli = System.currentTimeMillis() - fileStartTimeMilli;
        FileStats fileStats = new FileStats(runtimeMilli, size, fileSize, rows);
        fileStatsList.add(fileStats);
        return fileStatsList;
    }

    private <T> int flush(List<BufferChannelContext<T>> buffers, String filePath) throws IOException {
        List<ChannelData<T>> chdataList = new ArrayList<>();
        for (BufferChannelContext<T> bufferChannelContext : buffers) {
            ChannelData<T> chdata = bufferChannelContext.buffer.flush(filePath);
            chdata.setChannelContext(bufferChannelContext.channelFlushContext);
            chdataList.add(chdata);
        }

        Flusher<T> flusher = buffers.get(0).buffer.createFlusher();
        SerializationResult res = flusher.serialize(chdataList, filePath);

        Pair<byte[], Integer> compressionResult =
                compressIfNeededAndPadChunk(
                        filePath,
                        res.chunkData,
                        Constants.ENCRYPTION_ALGORITHM_BLOCK_SIZE_BYTES,
                        bdecVersion == Constants.BdecVersion.ONE);

        return compressionResult.getSecond();
    }

    private static List<ColumnMetadata> createColumns(boolean scale) {
        List<ColumnMetadata> columns = new ArrayList<>();

        //columns.add(startBuild("bl", ColumnPhysicalType.SB1, ColumnLogicalType.BOOLEAN).build());

        columns.add(createFixedNumberColumn(1, 2, scale ? 1 : 0));
        columns.add(createFixedNumberColumn(2, 4, scale ? 2 : 0));
        columns.add(createFixedNumberColumn(4, 9, scale ? 4 : 0));
        columns.add(createFixedNumberColumn(8, 18, scale ? 7 : 0));
        columns.add(createFixedNumberColumn(16, 38, scale ? 15 : 0));

        columns.add(startBuild("num_float", ColumnPhysicalType.DOUBLE, ColumnLogicalType.REAL).build());

        columns.add(
                startBuild("str", ColumnPhysicalType.LOB, ColumnLogicalType.TEXT).length(256).build());

        columns.add(
                startBuild("bin", ColumnPhysicalType.LOB, ColumnLogicalType.BINARY).byteLength(256).build());

//        columns.add(startBuild("var", ColumnPhysicalType.LOB, ColumnLogicalType.VARIANT).build());
//        columns.add(startBuild("obj", ColumnPhysicalType.LOB, ColumnLogicalType.OBJECT).build());
//        columns.add(startBuild("arr", ColumnPhysicalType.LOB, ColumnLogicalType.ARRAY).build());
//
//        columns.add(startBuild("epochdays", ColumnPhysicalType.SB4, ColumnLogicalType.DATE).build());
//        columns.add(
//                startBuild("timesec", ColumnPhysicalType.SB4, ColumnLogicalType.TIME).scale(0).build());
//        columns.add(
//                startBuild("timenano", ColumnPhysicalType.SB8, ColumnLogicalType.TIME).scale(9).build());
//        columns.add(
//                startBuild("epochsec_ntz", ColumnPhysicalType.SB8, ColumnLogicalType.TIMESTAMP_NTZ)
//                        .scale(0)
//                        .build());
//        columns.add(
//                startBuild("epochnano_ntz", ColumnPhysicalType.SB16, ColumnLogicalType.TIMESTAMP_NTZ)
//                        .scale(9)
//                        .build());
//        columns.add(
//                startBuild("epochsec_ltz", ColumnPhysicalType.SB8, ColumnLogicalType.TIMESTAMP_LTZ)
//                        .scale(0)
//                        .build());
//        columns.add(
//                startBuild("epochnano_ltz", ColumnPhysicalType.SB16, ColumnLogicalType.TIMESTAMP_LTZ)
//                        .scale(9)
//                        .build());
//        columns.add(
//                startBuild("epochsec_tz", ColumnPhysicalType.SB8, ColumnLogicalType.TIMESTAMP_TZ)
//                        .scale(0)
//                        .build());
//        columns.add(
//                startBuild("epochnano_tz", ColumnPhysicalType.SB16, ColumnLogicalType.TIMESTAMP_TZ)
//                        .scale(9)
//                        .build());
//
        return columns;
    }

    private static Map<String, Object> getRandomRow(Random r) {
        Map<String, Object> row = new HashMap<>();

        row.put("num_2_1", nullOr(r, () -> r.nextInt(100) / 10.0));
        row.put("num_4_2", nullOr(r, () -> r.nextInt(10000) / 100.0));
        row.put("num_9_4", nullOr(r, () -> r.nextInt(1000000000) / Math.pow(10, 4)));
        row.put("num_18_7", nullOr(r, () -> nextLongOfPrecision(r, 18) / Math.pow(10, 7)));
        row.put(
                "num_38_15",
                nullOr(
                        r,
                        () ->
                                new BigDecimal(
                                        "" + nextLongOfPrecision(r, 18) + "." + Math.abs(nextLongOfPrecision(r, 15)))));

        row.put("num_float", nullOr(r, () -> nextFloat(r)));
        row.put("str", nullOr(r, () -> nextString(r)));
        row.put("bin", nullOr(r, () -> nextBytes(r)));
//        row.put("bl", nullOr(r, r::nextBoolean));
//        row.put("var", nullOr(r, () -> nextJson(r)));
//        row.put("obj", nullOr(r, () -> nextJson(r)));
//        row.put("arr", nullOr(r, () -> Arrays.asList(r.nextInt(100), r.nextInt(100), r.nextInt(100))));
//
//        row.put("epochdays", nullOr(r, () -> randomDate(r)));
//        row.put("timesec", nullOr(r, () -> randomTime(r, 0)));
//        row.put("timenano", nullOr(r, () -> randomTime(r, 9)));
//        row.put("epochsec_ntz", nullOr(r, () -> randomTimestamp(r, 0, false)));
//        row.put("epochnano_ntz", nullOr(r, () -> randomTimestamp(r, 9, false)));
//        row.put("epochsec_ltz", nullOr(r, () -> randomTimestamp(r, 0, false)));
//        row.put("epochnano_ltz", nullOr(r, () -> randomTimestamp(r, 9, false)));
//        row.put("epochsec_tz", nullOr(r, () -> randomTimestamp(r, 0, true)));
//        row.put("epochnano_tz", nullOr(r, () -> randomTimestamp(r, 9, true)));
        return row;
    }

    private static <T> T nullOr(Random r, Supplier<T> value) {
        return r.nextBoolean() ? value.get() : null;
    }

    private static String randomTime(Random r, int scale) {
        String time = String.format("%d:%d:%d", r.nextInt(13), r.nextInt(60), r.nextInt(60));
        if (scale > 0) {
            time += "." + r.nextInt((int) Math.pow(10, scale) - 1);
        }
        return time;
    }

    private static String randomDate(Random r) {
        return String.format("%d-%d-%d", r.nextInt(200) + 1980, r.nextInt(12) + 1, r.nextInt(27) + 1);
    }

    private static String randomTimestamp(Random r, int scale, boolean zoned) {
        String ts = randomDate(r) + " " + randomTime(r, scale);
        if (zoned) {
            ts += String.format(" -0%d00", r.nextInt(9));
        }
        return ts;
    }

    private static ColumnMetadata createFixedNumberColumn(int byteLength, int precision, int scale) {
        ColumnPhysicalType pt;
        switch (byteLength) {
            case 1:
                pt = ColumnPhysicalType.SB1;
                break;
            case 2:
                pt = ColumnPhysicalType.SB2;
                break;
            case 4:
                pt = ColumnPhysicalType.SB4;
                break;
            case 8:
                pt = ColumnPhysicalType.SB8;
                break;
            case 16:
                pt = ColumnPhysicalType.SB16;
                break;
            default:
                throw new IllegalArgumentException("Unexpected fixed number byte length: " + byteLength);
        }
        return startBuild(String.format("num_%d_%d", precision, scale), pt, ColumnLogicalType.FIXED)
                .byteLength(byteLength)
                .precision(precision)
                .scale(scale)
                .build();
    }

    private static ColumnMetadataBuilder startBuild(
            String name, ColumnPhysicalType pt, ColumnLogicalType lt) {
        return ColumnMetadataBuilder
                .newBuilder()
                .name(name.toUpperCase(Locale.ROOT))
                .nullable(true)
                .physicalType(pt.toString())
                .logicalType(lt.toString());
    }

    private static long nextLongOfPrecision(Random r, int precision) {
        return r.nextLong() % Math.round(Math.pow(10, precision));
    }

    private static String nextString(Random r) {
        return new String(nextBytes(r));
    }

    private static byte[] nextBytes(Random r) {
        byte[] bin = new byte[128];
        r.nextBytes(bin);
        for (int i = 0; i < bin.length; i++) {
            bin[i] = (byte) (Math.abs(bin[i]) % 25 + 97); // ascii letters
        }
        return bin;
    }

    private static double nextFloat(Random r) {
        return (r.nextLong() % Math.round(Math.pow(10, 10))) / 100000d;
    }

    private static String nextJson(Random r) {
        return String.format(
                "{ \"%s\": %d, \"%s\": \"%s\", \"%s\": null, \"%s\": { \"%s\": %f, \"%s\": \"%s\", \"%s\":"
                        + " null } }",
                nextString(r),
                r.nextInt(),
                nextString(r),
                nextString(r),
                nextString(r),
                nextString(r),
                nextString(r),
                r.nextFloat(),
                nextString(r),
                nextString(r),
                nextString(r));
    }
}
