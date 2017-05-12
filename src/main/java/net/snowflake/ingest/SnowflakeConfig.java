package net.snowflake.ingest;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *  Encapsulates required information to connect to a snowflake database
 * @author vganesh
 */
public class SnowflakeConfig
{
  private final Map<String, String> config;

  /**
   *  Credentials required to authorize with Snowflake
   *  Clients will need all this information to connect.
   * @param account
   * @param user
   * @param host
   * @param scheme
   * @param port
   * @param password
   */
  public SnowflakeConfig(String account, String user, String host, String scheme,
                    String port, String password)
  {
    HashMap<String, String> tmp = new HashMap<String, String> ();
    tmp.put("account", account);
    tmp.put("user", user);
    tmp.put("host", host);
    tmp.put("scheme", scheme);
    tmp.put("port", port);
    tmp.put("password", password);

    config = Collections.unmodifiableMap(tmp);
  }

  public String Account()
  {
    if (config.containsKey("account"))
    {
      return config.get("account");
    }
    return "testaccount";
  }
  public String User()
  {
    if (config.containsKey("user"))
    {
      return config.get("user");
    }
    return "snowman";
  }
  public String Host()
  {
    if (config.containsKey("host"))
    {
      return config.get("host");
    }
    return "localhost";
  }
  public String Scheme()
  {
    if (config.containsKey("scheme"))
    {
      return config.get("scheme");
    }
    return "http";
  }
  public int Port()
  {
    if (config.containsKey("port")) {
      return Integer.parseInt(config.get("port"));
    }

    return 8080;
  }

  public String Password()
  {
    return config.get("password");
  }
}
