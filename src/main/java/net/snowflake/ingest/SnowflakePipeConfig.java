package net.snowflake.ingest;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * All information associated with a given pipe
 * @author vganesh
 */
public class SnowflakePipeConfig
{
  private final Map<String, String> config;
  public SnowflakePipeConfig(String database, String schema, String stage,
                             String table, String pipeName)
  {
    HashMap<String, String> tmp = new HashMap<String, String>();
    tmp.put("database", database);
    tmp.put("schema", schema);
    tmp.put("stage", stage);
    tmp.put("table", table);
    tmp.put("name", pipeName);

    config = Collections.unmodifiableMap(tmp);
  }

  public String Database()
  {
    return config.get("database");
  }

  public String Schema()
  {
    return config.get("schema");
  }

  public String Stage()
  {
    return config.get("stage");
  }

  public String Table()
  {
    return config.get("table");
  }

  public String Name()
  {
    return config.get("name");
  }
}
