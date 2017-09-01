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
  private String pipe;
  private String startTimeInclusive;
  private String endTimeExclusive;
  private String rangeStartTime;
  private String rangeEndTime;
  private boolean completeResult;

  /**the list of file status objects*/
  public List<HistoryResponse.FileEntry> files;
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

  /**fully qualified pipe name*/
  public String getPipe()
  {
    return pipe;
  }

  /**fully qualified pipe name*/
  public void setPipe(String pipe)
  {
    this.pipe = pipe;
  }

  /**startTimeInclusive (timestamp in ISO-8601 format) provided in the request.*/
  public String getStartTimeInclusive()
  {
    return startTimeInclusive;
  }

  /**startTimeInclusive (timestamp in ISO-8601 format) provided in the request.*/
  public void setStartTimeInclusive(String startTimeInclusive)
  {
    this.startTimeInclusive = startTimeInclusive;
  }

  /**endTimeExclusive (timestamp in ISO-8601 format) provided in the request.*/
  public String getEndTimeExclusive()
  {
    return endTimeExclusive;
  }

  /**endTimeExclusive (timestamp in ISO-8601 format) provided in the request.*/
  public void setEndTimeExclusive(String endTimeExclusive)
  {
    this.endTimeExclusive = endTimeExclusive;
  }

  /**lastInsertTime (timestamp in ISO-8601 format) of the oldest entry in the
   * files included in the response.*/
  public String getRangeStartTime()
  {
    return rangeStartTime;
  }

  /**lastInsertTime (timestamp in ISO-8601 format) of the oldest entry in the
   * files included in the response.*/
  public void setRangeStartTime(String rangeStartTime)
  {
    this.rangeStartTime = rangeStartTime;
  }

  /**lastInsertTime (timestamp in ISO-8601 format) of the latest entry in the
   * files included in the response.*/
  public String getRangeEndTime()
  {
    return rangeEndTime;
  }

  /**lastInsertTime (timestamp in ISO-8601 format) of the latest entry in the
   * files included in the response.*/
  public void setRangeEndTime(String rangeEndTime)
  {
    this.rangeEndTime = rangeEndTime;
  }

  /**False if the report is incomplete, i.e. the number of entries in the
   * specified time range exceeds the 10,000 entry limit. If false, the user
   * can specify the current``rangeEndTime`` value as the startTimeInclusive
   * value for the next request to proceed to the next set of entries.*/
  public boolean isCompleteResult()
  {
    return completeResult;
  }

  public void setCompleteResult(boolean completeResult)
  {
    this.completeResult = completeResult;
  }
}
