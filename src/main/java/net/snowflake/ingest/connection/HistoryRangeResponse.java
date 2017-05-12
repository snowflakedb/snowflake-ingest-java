package net.snowflake.ingest.connection;

import java.util.List;

/**
 *
 * HistoryRangeResponse - response containing all history for a given pipe
 *                        within a given date range, received from range history
 *                        endpoint.
 * Created by vganesh on 7/18/17.
 */
public class HistoryRangeResponse
{
  public List<HistoryResponse.FileEntry> files;

  public String pipe;
  public String rangeBeginInclusive;
  public String rangeEndExclusive;

  @Override
  public String toString()
  {
    StringBuilder result = new StringBuilder();
    result.append("\nHistory Range Result:\n");
    result.append("\nStart Time: ").append(rangeBeginInclusive);
    result.append("\nEnd Time: ").append(rangeEndExclusive);
    result.append("\nPipe: ").append(pipe).append("\n");
    String sep = "";
    for (HistoryResponse.FileEntry file: files)
    {
      result.append(sep).append("{\n");
      result.append(file.toString());
      result.append("}");
      sep = ",\n";
    }
    result.append("\n-------------\n");
    return result.toString();
  }
}
