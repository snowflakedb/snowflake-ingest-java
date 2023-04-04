package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.parquet.hadoop.BdecParquetReader;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static net.snowflake.ingest.streaming.internal.BlobBuilder.compressIfNeededAndPadChunk;
import static net.snowflake.ingest.streaming.internal.ParquetRowBuffer.BufferingType.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ParquetWriterTest {
    @Test
    public void testDataTypes() throws IOException {
        final int rowNumber = 1000;

        AbstractRowBuffer<?> buffer = createBuffer(Constants.BdecVersion.THREE, PARQUET_BUFFERS);

        List<ColumnMetadata> columns = createSchema();
        buffer.setupSchema(columns);

        //String expected = "null, null, null, 3, null, 5, null, null, null, null, 10, null, 12, null, 14, null, null, null, 18, 19, 20, 21, null, 23, 24, null, null, 27, null, null, 30, 31, null, 33, 34, 35, null, null, null, 39, null, 41, null, null, 44, 45, 46, 47, 48, null, 50, null, 52, null, 54, 55, 56, 57, 58, null, null, 61, null, null, 64, null, null, 67, 68, 69, 70, null, 72, null, null, null, 76, null, null, null, 80, 81, 82, 83, null, null, 86, 87, 88, 89, null, null, 92, null, null, null, 96, 97, 98, null, null, null, null, null, null, null, null, null, 108, 109, 110, null, 112, null, null, 115, 116, null, 118, 119, null, 121, null, null, null, null, null, 127, null, null, 130, null, 132, 133, 134, null, 136, null, 138, 139, null, null, null, 143, null, 145, 146, null, 148, 149, 150, 151, null, 153, 154, null, 156, 157, 158, 159, null, 161, null, 163, 164, 165, null, null, 168, null, null, null, 172, 173, null, null, null, null, 178, null, 180, 181, 182, null, null, null, 186, 187, 188, 189, null, null, null, 193, null, null, null, null, null, null, 200, 201, null, null, null, 205, 206, null, 208, null, null, 211, 212, 213, 214, null, null, null, null, null, 220, 221, null, 223, 224, null, null, 227, 228, 229, 230, null, 232, 233, 234, 235, null, null, 238, 239, 240, null, 242, null, 244, 245, 246, 247, null, null, 250, 251, 252, null, null, 255, null, 257, 258, 259, 260, null, null, null, null, 265, 266, 267, null, 269, 270, 271, null, 273, null, 275, null, 277, null, 279, 280, 281, 282, 283, null, null, 286, 287, null, null, null, null, null, null, null, null, 296, 297, 298, 299, null, null, 302, null, 304, 305, 306, null, null, null, null, null, 312, 313, null, null, 316, 317, null, 319, 320, 321, null, null, 324, 325, null, 327, 328, null, 330, 331, 332, 333, 334, null, null, null, null, 339, 340, 341, null, null, 344, null, null, null, null, 349, null, null, null, 353, null, null, null, 357, 358, 359, null, null, 362, null, null, null, null, null, null, null, 370, 371, 372, null, 374, 375, 376, null, 378, null, null, 381, null, null, null, 385, 386, 387, 388, null, 390, null, 392, 393, null, null, 396, 397, null, 399, null, 401, 402, null, null, 405, 406, null, null, 409, 410, 411, 412, null, 414, null, 416, 417, 418, null, null, 421, 422, 423, 424, null, null, null, 428, null, null, 431, 432, 433, null, 435, 436, 437, 438, 439, 440, null, 442, 443, null, 445, null, 447, null, 449, 450, null, null, null, 454, 455, 456, null, null, null, 460, 461, null, 463, null, null, null, 467, null, null, 470, 471, null, null, 474, 475, 476, null, null, null, 480, null, null, 483, 484, 485, null, null, 488, 489, null, null, 492, 493, 494, null, null, null, null, null, 500, 501, null, 503, null, 505, null, null, 508, null, null, null, null, 513, null, 515, 516, 517, 518, 519, null, null, null, null, 524, 525, 526, 527, 528, null, 530, 531, null, null, 534, 535, null, null, null, null, 540, 541, null, null, null, 545, null, 547, null, 549, null, null, 552, 553, 554, 555, 556, 557, 558, 559, 560, null, null, null, 564, null, null, null, 568, 569, null, null, 572, null, null, null, null, null, null, 579, 580, 581, null, 583, 584, null, 586, null, null, null, null, null, 592, null, 594, 595, null, null, 598, null, null, 601, 602, null, 604, 605, null, null, 608, null, null, 611, 612, 613, 614, null, null, 617, 618, 619, null, 621, null, null, null, 625, null, null, 628, null, null, 631, 632, 633, null, null, null, 637, 638, null, null, null, null, 643, 644, 645, null, null, 648, 649, 650, null, 652, 653, null, 655, null, 657, 658, null, 660, 661, 662, null, null, null, null, 667, 668, null, null, null, 672, 673, 674, null, null, 677, 678, 679, null, 681, null, 683, null, null, null, null, 688, null, 690, 691, null, null, null, null, 696, 697, null, null, 700, 701, 702, null, null, 705, null, null, null, 709, null, 711, null, null, null, 715, 716, null, null, 719, null, 721, null, 723, null, null, 726, null, null, 729, null, 731, 732, 733, null, 735, null, null, null, null, 740, null, 742, null, null, null, null, null, 748, null, null, null, 752, null, 754, null, null, 757, 758, 759, 760, 761, 762, null, null, 765, null, null, null, null, null, null, 772, 773, 774, 775, 776, 777, null, 779, null, null, null, 783, 784, 785, null, 787, null, 789, 790, null, null, 793, null, null, null, 797, null, 799, 800, null, 802, null, 804, 805, null, 807, 808, null, null, null, 812, 813, 814, null, 816, null, null, 819, null, 821, 822, null, 824, 825, null, 827, 828, 829, null, 831, null, null, 834, 835, 836, null, null, null, 840, null, 842, 843, 844, 845, null, null, null, 849, 850, 851, null, null, 854, null, null, null, 858, null, null, 861, null, null, 864, null, null, null, 868, null, 870, null, 872, 873, null, 875, 876, 877, null, 879, null, 881, null, 883, 884, null, 886, 887, 888, 889, null, null, null, 893, 894, 895, 896, null, 898, 899, 900, null, null, null, 904, 905, 906, null, 908, null, 910, null, null, 913, 914, null, 916, null, null, null, null, null, null, 923, 924, 925, null, null, 928, 929, 930, 931, 932, null, null, 935, 936, null, 938, null, null, null, null, null, null, 945, null, 947, 948, null, null, 951, null, 953, null, 955, null, null, 958, null, 960, null, null, null, 964, 965, 966, 967, 968, null, 970, 971, 972, null, 974, null, null, 977, null, null, 980, 981, 982, null, 984, null, 986, 987, null, 989, null, null, null, 993, 994, null, null, null, 998, 999";
        //List<Boolean> nulls = Arrays.stream(expected.split(", ")).map(s -> s.equals("null")).collect(Collectors.toList());
        List<String> strData = new ArrayList<>();
        List<Integer> intData = new ArrayList<>();
        List<Boolean> bData = new ArrayList<>();
        Random r = new Random();
        for (int val = 0; val < rowNumber; val++) {
            Map<String, Object> row = new HashMap<>();

            String strValue = r.nextBoolean() ? null : Integer.toString(val);
            strData.add(strValue);
            row.put("c1", strValue);

            Integer intValue = r.nextBoolean() ? null : val;
            intData.add(intValue);
            row.put("c2", intValue);

            Boolean bValue = r.nextBoolean() ? null : val % 2 == 0;
            bData.add(bValue);
            row.put("c3", bValue);

            InsertValidationResponse response = buffer.insertRows(Collections.singletonList(row), Integer.toString(val));
            if (response.hasErrors()) {
                throw response.getInsertErrors().get(0).getException();
            }
        }

        byte[] data = flush(Constants.BdecVersion.THREE, buffer);
        try (BdecParquetReader reader = new BdecParquetReader(data)) {
            List<String> strRecords = new ArrayList<>();
            List<Integer> intRecords = new ArrayList<>();
            List<Boolean> bRecords = new ArrayList<>();
            for (List<Object> record = reader.read(); record != null; record = reader.read()) {
                assertEquals(columns.size(), record.size());

                Object value = record.get(0);
                assertTrue(value == null || value instanceof byte[]);
                String strValue = value == null ? null : new String((byte[]) value, StandardCharsets.UTF_8);
                strRecords.add(strValue);

                value = record.get(1);
                assertTrue(value == null || value instanceof Integer);
                Integer intValue = value == null ? null : (int) value;
                intRecords.add(intValue);

                value = record.get(2);
                assertTrue(value == null || value instanceof Boolean);
                Boolean bValue = value == null ? null : (boolean) value;
                bRecords.add(bValue);
            }
            assertEquals(strData, strRecords);
            assertEquals(intData, intRecords);
            assertEquals(bData, bRecords);
        } catch (IOException e) {
            throw new SFException(ErrorCode.INTERNAL_ERROR, "Failed to read parquet file", e);
        }
    }

    @Test
    public void testXxx() throws IOException {
        final int rowNumber = 6000;
        final int repeat = 10;
        final Constants.BdecVersion version = Constants.BdecVersion.THREE;

        AbstractRowBuffer<?> buffer = createBuffer(version, PARQUET_BUFFERS);

        List<ColumnMetadata> columns = new ArrayList<>();
        for (int i = 1; i <= 30; i++) {
            columns.add(ColumnMetadataBuilder.newBuilder()
                    .name("c" + i)
                    .logicalType("TEXT")
                    .physicalType("LOB")
                    .nullable(true)
                    .length(56)
                    .build());
        }
        buffer.setupSchema(columns);

        long buffering = 0;
        long flushing = 0;
        for (int r = 0; r < repeat; r++) {
            long start = System.currentTimeMillis();
            for (int val = 0; val < rowNumber; val++) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= 30; i++) {
                    row.put("c" + i, "XXXXXXXXXXXXXXXX");
                }
                InsertValidationResponse response = buffer.insertRows(Collections.singletonList(row), Integer.toString(val));
                if (response.hasErrors()) {
                    throw response.getInsertErrors().get(0).getException();
                }
            }
            buffering += System.currentTimeMillis() - start;

            flush(version, buffer);
            flushing += System.currentTimeMillis() - start;
        }
        System.out.println("buffering: " + buffering / repeat);
        System.out.println("flushing: " + flushing / repeat);
    }

    private static List<ColumnMetadata> createSchema() {
        ColumnMetadata testCol1 =
                ColumnMetadataBuilder.newBuilder()
                        .name("c1")
                        .logicalType("TEXT")
                        .physicalType("LOB")
                        .nullable(true)
                        .length(56)
                        .build();
        ColumnMetadata testCol2 =
                ColumnMetadataBuilder.newBuilder()
                        .name("c2")
                        .logicalType("FIXED")
                        .physicalType("SB4")
                        .scale(0)
                        .precision(9)
                        .nullable(true)
                        .build();
        ColumnMetadata testCol3 =
                ColumnMetadataBuilder.newBuilder()
                        .name("c3")
                        .logicalType("BOOLEAN")
                        .physicalType("SB1")
                        .nullable(true)
                        .build();
        List<ColumnMetadata> columns = Arrays.asList(testCol1, testCol2, testCol3);
        return columns;
    }

    private static <T> byte[] flush(Constants.BdecVersion bdecVersion, AbstractRowBuffer<T> buffer) throws IOException {
        ChannelFlushContext channelFlushContext = new ChannelFlushContext(
                "name", "dbName", "schemaName", "tableName", 0L, "encryptionKey", 0L);
        ChannelData<T> chunkData = buffer.flush("filePath");
        chunkData.setChannelContext(channelFlushContext);
        Flusher.SerializationResult result = buffer.createFlusher().serialize(Collections.singletonList(chunkData), "filePath");
        byte[] bytes = compressIfNeededAndPadChunk(
                "filePath",
                result.chunkData,
                Constants.ENCRYPTION_ALGORITHM_BLOCK_SIZE_BYTES,
                bdecVersion == Constants.BdecVersion.ONE).getFirst();
        //System.out.println("size: " + bytes.length);
        return bytes;
    }

    private static <T> AbstractRowBuffer<T> createBuffer(Constants.BdecVersion version, ParquetRowBuffer.BufferingType bufferingType) {
        return AbstractRowBuffer.createRowBuffer(
                OpenChannelRequest.OnErrorOption.CONTINUE,
                ZoneId.systemDefault(),
                new RootAllocator(),
                version,
                "parquet_test_channel",
                m -> {},
                new ChannelRuntimeState("0", 0, true),
                false,
                bufferingType);
    }


}
