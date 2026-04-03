/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/core/URLUtil.java
 *
 * Permitted differences: package declaration, import swaps for already-replicated classes,
 * @SnowflakeJdbcInternalApi annotation removed.
 * SFSession.SF_QUERY_REQUEST_ID inlined as a local constant (value: "requestId").
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.annotation.Nullable;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLogger;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLoggerFactory;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

public class URLUtil {

  private static final SFLogger logger = SFLoggerFactory.getLogger(URLUtil.class);

  // Inlined from SFSession.SF_QUERY_REQUEST_ID
  private static final String SF_QUERY_REQUEST_ID = "requestId";

  static final String validURLPattern =
      "^http(s?)\\:\\/\\/[0-9a-zA-Z]([-.\\w]*[0-9a-zA-Z@:])*(:(0-9)*)*(\\/?)([a-zA-Z0-9\\-\\.\\?\\,\\&\\(\\)\\/\\\\\\+&%\\$#_=@]*)?$";
  static final Pattern pattern = Pattern.compile(validURLPattern);

  public static boolean isValidURL(String url) {
    try {
      Matcher matcher = pattern.matcher(url);
      return matcher.find();
    } catch (PatternSyntaxException pex) {
      logger.debug("The URL REGEX is invalid. Falling back to basic sanity test");
      try {
        new URL(url).toURI();
        return true;
      } catch (MalformedURLException mex) {
        logger.debug("The URL " + url + ", is invalid");
        return false;
      } catch (URISyntaxException uex) {
        logger.debug("The URL " + url + ", is invalid");
        return false;
      }
    }
  }

  @Nullable
  public static String urlEncode(String target) throws UnsupportedEncodingException {
    String encodedTarget;
    try {
      encodedTarget = URLEncoder.encode(target, StandardCharsets.UTF_8.toString());
    } catch (UnsupportedEncodingException uex) {
      logger.debug("The string to be encoded- " + target + ", is invalid");
      return null;
    }
    return encodedTarget;
  }

  public static String getRequestId(URI uri) {
    return URLEncodedUtils.parse(uri, StandardCharsets.UTF_8).stream()
        .filter(p -> p.getName().equals(SF_QUERY_REQUEST_ID))
        .findFirst()
        .map(NameValuePair::getValue)
        .orElse(null);
  }

  public static String getRequestIdLogStr(URI uri) {
    String requestId = getRequestId(uri);

    return requestId == null ? "" : "[requestId=" + requestId + "] ";
  }
}
