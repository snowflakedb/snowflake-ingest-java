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
  private static final String[] columnNames =
      new String[] {
        "I1", "I2", "I3", "I4", "I5", "I6", "I7", "I8", "I9", "I10", "I11", "I12", "I13", "C1",
        "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "C10", "C11", "C12", "C13", "C14", "C15",
        "C16", "C17", "C18", "C19", "C20", "C21", "C22", "C23", "C24", "C25", "C26"
      };

  public static void main(String[] args) throws IOException {
    System.out.println("Hello");
    List<Map<String, Object>> data = readData();
    System.out.println("data's length=" + data.size());
  }

  private static List<Map<String, Object>> readData() throws IOException {
    List<Map<String, Object>> records = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader("criteo.csv"))) {
      String line;
      while ((line = br.readLine()) != null) {
        records.add(getRowAsMap(line));
      }
    }
    return records;
  }

  private static Map<String, Object> getRowAsMap(String line) {
    Map<String, Object> rowMap = new HashMap<>();
    String[] values = line.split((COMMA_DELIMITER));
    for (int i = 0; i < values.length; i++) {
      rowMap.put(columnNames[i], values[i]);
    }
    return rowMap;
  }
}
