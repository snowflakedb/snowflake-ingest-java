package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_CHANNEL_STATUS;
import static net.snowflake.ingest.streaming.internal.StreamingIngestUtils.executeWithRetries;
import static net.snowflake.ingest.utils.Constants.CHANNEL_STATUS_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST;
import static net.snowflake.ingest.utils.Constants.RESPONSE_SUCCESS;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.ArrayList;
import net.snowflake.client.jdbc.internal.apache.commons.io.IOUtils;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.connection.RequestBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class StreamingIngestUtilsTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testSuccessNoRetries() throws Exception {
    ChannelsStatusResponse response = new ChannelsStatusResponse();
    response.setStatusCode(0L);
    response.setMessage("honk");
    response.setChannels(new ArrayList<>());
    String responseString = objectMapper.writeValueAsString(response);

    HttpClient httpClient = Mockito.mock(HttpClient.class);
    HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
    StatusLine statusLine = Mockito.mock(StatusLine.class);
    HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
    Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);
    Mockito.when(httpEntity.getContent()).thenReturn(IOUtils.toInputStream(responseString));
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

    RequestBuilder requestBuilder =
        Mockito.spy(
            new RequestBuilder(TestUtils.getHost(), TestUtils.getUser(), TestUtils.getKeyPair()));

    executeWithRetries(
        ChannelsStatusResponse.class,
        CHANNEL_STATUS_ENDPOINT,
        "{}",
        "channel status",
        STREAMING_CHANNEL_STATUS,
        httpClient,
        requestBuilder);

    Mockito.verify(requestBuilder, Mockito.times(1))
        .generateStreamingIngestPostRequest(Mockito.anyString(), Mockito.any(), Mockito.any());
  }

  InputStream getInputStream(String value) {
    return IOUtils.toInputStream(value);
  }

  @Test
  public void testRetries() throws Exception {
    ChannelsStatusResponse response = new ChannelsStatusResponse();
    response.setStatusCode(RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST);
    //    response.setStatusCode(7L);

    response.setMessage("honk");
    response.setChannels(new ArrayList<>());
    String responseString = objectMapper.writeValueAsString(response);

    HttpClient httpClient = Mockito.mock(HttpClient.class);
    HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
    StatusLine statusLine = Mockito.mock(StatusLine.class);
    HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
    Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);

    Mockito.when(httpEntity.getContent())
        .thenAnswer(
            new Answer<InputStream>() {
              @Override
              public InputStream answer(InvocationOnMock invocation) throws Throwable {
                return IOUtils.toInputStream(responseString);
              }
            });

    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

    RequestBuilder requestBuilder =
        Mockito.spy(
            new RequestBuilder(TestUtils.getHost(), TestUtils.getUser(), TestUtils.getKeyPair()));

    ChannelsStatusResponse result =
        executeWithRetries(
            ChannelsStatusResponse.class,
            CHANNEL_STATUS_ENDPOINT,
            "{}",
            "channel status",
            STREAMING_CHANNEL_STATUS,
            httpClient,
            requestBuilder);

    Mockito.verify(requestBuilder, Mockito.times(4))
        .generateStreamingIngestPostRequest(Mockito.anyString(), Mockito.any(), Mockito.any());
    Assert.assertEquals("honk", result.getMessage());
  }

  @Test
  public void testRetriesRecovery() throws Exception {
    ChannelsStatusResponse errorResponse = new ChannelsStatusResponse();
    errorResponse.setStatusCode(RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST);

    errorResponse.setMessage("honkFailure");
    errorResponse.setChannels(new ArrayList<>());
    String errorResponseString = objectMapper.writeValueAsString(errorResponse);

    ChannelsStatusResponse successfulResponse = new ChannelsStatusResponse();
    successfulResponse.setStatusCode(RESPONSE_SUCCESS);

    successfulResponse.setMessage("honkSuccess");
    successfulResponse.setChannels(new ArrayList<>());
    String successfulResponseString = objectMapper.writeValueAsString(successfulResponse);

    HttpClient httpClient = Mockito.mock(HttpClient.class);
    HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
    StatusLine statusLine = Mockito.mock(StatusLine.class);
    HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
    Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);

    Mockito.when(httpEntity.getContent())
        .thenReturn(
            IOUtils.toInputStream(errorResponseString),
            IOUtils.toInputStream(errorResponseString),
            IOUtils.toInputStream(successfulResponseString));
    Mockito.when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);

    RequestBuilder requestBuilder =
        Mockito.spy(
            new RequestBuilder(TestUtils.getHost(), TestUtils.getUser(), TestUtils.getKeyPair()));

    ChannelsStatusResponse result =
        executeWithRetries(
            ChannelsStatusResponse.class,
            CHANNEL_STATUS_ENDPOINT,
            "{}",
            "channel status",
            STREAMING_CHANNEL_STATUS,
            httpClient,
            requestBuilder);

    Mockito.verify(requestBuilder, Mockito.times(3))
        .generateStreamingIngestPostRequest(Mockito.anyString(), Mockito.any(), Mockito.any());

    Assert.assertEquals("honkSuccess", result.getMessage());
  }
}
