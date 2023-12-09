package net.snowflake.ingest.connection;

import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.ingest.utils.SnowflakeURL;

public class RequestBuilderFactory {
    public RequestBuilder build(SnowflakeURL url,
                         String userName,
                         Object credential,
                         CloseableHttpClient httpClient,
                         String clientName) {
        return new RequestBuilder(url, userName, credential, httpClient, clientName);
    }
}
