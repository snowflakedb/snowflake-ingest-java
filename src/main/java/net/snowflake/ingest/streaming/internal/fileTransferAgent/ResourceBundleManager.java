/*
 * Replicated from snowflake-jdbc-thin (v3.25.1):
 *   net.snowflake.client.jdbc.internal.snowflake.common.core.ResourceBundleManager
 * Originally from net.snowflake:snowflake-common (no public source URL).
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.Set;

public class ResourceBundleManager {
  private static Map<String, ResourceBundleManager> resourceManagers =
      new HashMap<String, ResourceBundleManager>();
  private static final Object lockObject = new Object();
  private final String bundleName;
  private ResourceBundle resourceBundle;

  private ResourceBundleManager(String bundleName) {
    this.bundleName = bundleName;
    this.reload();
  }

  public void reload() {
    ResourceBundle bundle = null;
    try {
      bundle = ResourceBundle.getBundle(this.bundleName, Locale.getDefault());
    } catch (Throwable ex1) {
      try {
        bundle = ResourceBundle.getBundle(this.bundleName);
      } catch (Throwable ex2) {
        throw new RuntimeException(
            "Can't load localized resource bundle due to "
                + ex1.toString()
                + " and can't load default resource bundle due to "
                + ex2.toString());
      }
    } finally {
      this.resourceBundle = bundle;
    }
  }

  public String getLocalizedMessage(String key) {
    if (this.resourceBundle == null) {
      throw new RuntimeException(
          "Localized messages from resource bundle '" + this.bundleName + "' not loaded.");
    }
    try {
      if (key == null) {
        throw new IllegalArgumentException("Message key cannot be null");
      }
      String message = this.resourceBundle.getString(key);
      if (message == null) {
        message = "!!" + key + "!!";
      }
      return message;
    } catch (MissingResourceException e) {
      return '!' + key + '!';
    }
  }

  public String getLocalizedMessage(String key, Object... args) {
    return MessageFormat.format(this.getLocalizedMessage(key), args);
  }

  public ResourceBundle getResourceBundle() {
    return this.resourceBundle;
  }

  public static ResourceBundleManager getSingleton(String bundleName) {
    if (resourceManagers.get(bundleName) != null) {
      return resourceManagers.get(bundleName);
    }
    synchronized (lockObject) {
      if (resourceManagers.get(bundleName) != null) {
        return resourceManagers.get(bundleName);
      }
      resourceManagers.put(bundleName, new ResourceBundleManager(bundleName));
    }
    return resourceManagers.get(bundleName);
  }

  public Set<String> getKeySet() {
    return this.resourceBundle.keySet();
  }
}
