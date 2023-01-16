package net.snowflake.ingest.streaming.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CriteoPerf {

  private static final String COMMA_DELIMITER = ",";
  /**
   * create or replace table STREAMING_INGEST_TEST_TABLE_CRITEO (i1 float, i2 int, i3 float, i4 float, i5 float, i6 float, i7 float, i8 float, i9 float, i10 float, i11 float, i12 float, i13 float,
   *                                                            c1 string, c2 string, c3 string, c4 string, c5 string, c6 string, c7 string, c8 string, c9 string, c10 string, c11 string, c12 string, c13 string, c14 string, c15 string, c16 string, c17 string, c18 string, c19 string, c20 string,
   *                                                            c21 string, c22 string, c23 string, c24 string, c25 string, c26 string);
   */
  private static final String[] columnNames =
      new String[] {
        "I1", "I2", "I3", "I4", "I5", "I6", "I7", "I8", "I9", "I10", "I11", "I12", "I13", "C1",
        "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "C10", "C11", "C12", "C13", "C14", "C15",
        "C16", "C17", "C18", "C19", "C20", "C21", "C22", "C23", "C24", "C25", "C26"
      };

  public static List<Map<String, Object>> readData() throws IOException {
    List<Map<String, Object>> records = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader("/home/ec2-user/criteo.csv"))) {
      String line;
      while ((line = br.readLine()) != null) {
        records.add(getRowAsMap(line));
      }
    }
    return records;
  }

  private static Map<String, Object> getRowAsMap(String line) {
    Map<String, Object> rowMap = new HashMap<>();
    String[] values = line.split((COMMA_DELIMITER), -1);
    assert values.length == columnNames.length;
    for (int i = 0; i < values.length; i++) {
      if(values[i] == null || values[i].toString().equals("NULL")) {
        values[i] = null;
      }
      rowMap.put(columnNames[i], values[i]);
    }
    return rowMap;
  }
}
