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
  public String startTimeInclusive;
  public String endTimeExclusive;
  public String rangeStartTime;
  public String rangeEndTime;
  public boolean completeResult;

  @Override
  public String toString()
  {
    StringBuilder result = new StringBuilder();
    result.append("\nHistory Range Result:\n");
    result.append("\nPipe: ").append(pipe);
    result.append("\nstartTimeInclusive: ").append(startTimeInclusive);
    result.append("\nendTimeExclusive: ").append(endTimeExclusive);
    result.append("\nComplete result: ").append(completeResult);
    result.append("\nrangeStartTime: ").append(rangeStartTime);
    result.append("\nrangeEndtime: ").append(rangeEndTime).append("\n");
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
