package net.snowflake.ingest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.AbstractMap;
/**
 * Created by vganesh on 5/11/17.
 */
public class SnowConfig
{
  private final Map<String, String> config;

  /**
   * Parses and loads config parameters from the snowflake config file.
   */
  private static class ConfigFileLoader {
    private Map<String, String> configParams = new HashMap<String, String>();

    private static Entry<String, String> parseConfigLine(String configParamLine)
    {
      String[] pair = configParamLine.split("=", 2);
      if (pair.length != 2)
      {
        throw new IllegalArgumentException(
                "Invalid config format: no '=' character is found in the line ["
                        + configParamLine + "].");
      }

      String configParamKey   = pair[0].trim().toLowerCase();
      String configParamValue = pair[1].trim();

      return new AbstractMap.SimpleImmutableEntry<String, String>(
              configParamKey, configParamValue);
    }

    private void load(Scanner scanner)
    {
      try
      {
        while(scanner.hasNextLine())
        {
          String line = scanner.nextLine().trim();

          // Empty or comment lines
          if (line.isEmpty() || line.startsWith("#"))
          {
            continue;
          }

          Entry<String, String> configParam = parseConfigLine(line);
          if (configParams.containsKey(configParam.getKey()))
          {
            throw new IllegalArgumentException(
                    "Duplicate config values for [" +
                            configParam.getKey() + "].");
          }

          configParams.put(configParam.getKey(), configParam.getValue());
          }
      }
      finally
      {
        scanner.close();
      }
    }

    public Map<String, String> parseConfigFile(Scanner scanner)
    {
      load(scanner);
      return new HashMap<String, String>(configParams);
    }
  }

  public Map<String, String> loadConfig(File file)
  {
    if (file == null)
    {
      throw new IllegalArgumentException(
              "Unable to load Snowflake config file: file is null.");
    }

    if (!file.exists() || !file.isFile())
    {
      throw new IllegalArgumentException(
              "Snowflake config file was not found under path: " +
                      file.getAbsolutePath());
    }

    FileInputStream fis = null;
    try
    {
      ConfigFileLoader configLoader = new ConfigFileLoader();
      fis = new FileInputStream(file);
      return configLoader.parseConfigFile(new Scanner(fis));
    }
    catch (IOException ioe)
    {
      throw new RuntimeException(
              "Unable to load Snowflake config file at: "
                      + file.getAbsolutePath(), ioe);
    }
    finally
    {
      if (fis != null)
      {
        try
        {
          fis.close();
        }
        catch (IOException ioe)
        {
        }
      }
    }
  }

  private File getConfigFile()
  {
    String userHome = System.getProperty("user.home");
    if (userHome == null)
    {
      throw new RuntimeException(
              "Unable to load Snowflake credentials " +
                      "'user.home' System property is not set.");
    }

    File configFile = new File(new File(userHome, ".snow"), "config");
    if (configFile.exists() && configFile.isFile())
    {
      return configFile;
    }
    return null;
  }

  public SnowConfig()
  {
    config = loadConfig(getConfigFile());
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
