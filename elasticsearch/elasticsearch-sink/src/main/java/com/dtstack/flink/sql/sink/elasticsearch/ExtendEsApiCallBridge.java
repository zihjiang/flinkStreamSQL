package com.dtstack.flink.sql.sink.elasticsearch;

import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.util.Preconditions;

import com.dtstack.flink.sql.sink.elasticsearch.table.ElasticsearchTableInfo;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author zihjiang
 * @date 2020/10/22 - 15:10
 */
public class ExtendEsApiCallBridge implements ElasticsearchApiCallBridge<RestHighLevelClient> {

    private static final Logger LOG = LoggerFactory.getLogger(ExtendEsApiCallBridge.class);

    private final List<HttpHost> HttpAddresses;

    protected ElasticsearchTableInfo esTableInfo;

    public ExtendEsApiCallBridge(List<HttpHost> httpAddresses, ElasticsearchTableInfo esTableInfo) {
        Preconditions.checkArgument(httpAddresses != null && !httpAddresses.isEmpty());
        this.HttpAddresses = httpAddresses;
        this.esTableInfo = esTableInfo;
    }

    @Override
    public RestHighLevelClient createClient(Map<String, String> clientConfig) {

        RestClientBuilder restClientBuilder = RestClient.builder(HttpAddresses.toArray(new HttpHost[HttpAddresses.size()]));
        if (esTableInfo.isAuthMesh()) {
            // 进行用户和密码认证
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(esTableInfo.getUserName(), esTableInfo.getPassword()));
            restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder ->
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }

        RestHighLevelClient rhlClient = new RestHighLevelClient(restClientBuilder);

        if (LOG.isInfoEnabled()) {
            LOG.info("Pinging Elasticsearch cluster via hosts {} ...", HttpAddresses);
        }

        try{
            if (!rhlClient.ping(RequestOptions.DEFAULT)) {
                throw new RuntimeException("There are no reachable Elasticsearch nodes!");
            }
        } catch (IOException e){
            LOG.warn("", e);
        }


        if (LOG.isInfoEnabled()) {
            LOG.info("Created Elasticsearch RestHighLevelClient connected to {}", HttpAddresses.toString());
        }

        return rhlClient;
    }


    @Override
    public BulkProcessor.Builder createBulkProcessorBuilder(RestHighLevelClient client, BulkProcessor.Listener listener) {

        return BulkProcessor.builder((request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener), listener);

    }

    @Override
    public Throwable extractFailureCauseFromBulkItemResponse(BulkItemResponse bulkItemResponse) {
        if (!bulkItemResponse.isFailed()) {
            return null;
        } else {
            return bulkItemResponse.getFailure().getCause();
        }
    }

    @Override
    public void configureBulkProcessorBackoff(
            BulkProcessor.Builder builder,
            @Nullable ElasticsearchSinkBase.BulkFlushBackoffPolicy flushBackoffPolicy) {

        BackoffPolicy backoffPolicy;
        if (flushBackoffPolicy != null) {
            switch (flushBackoffPolicy.getBackoffType()) {
                case CONSTANT:
                    backoffPolicy = BackoffPolicy.constantBackoff(
                            new TimeValue(flushBackoffPolicy.getDelayMillis()),
                            flushBackoffPolicy.getMaxRetryCount());
                    break;
                case EXPONENTIAL:
                default:
                    backoffPolicy = BackoffPolicy.exponentialBackoff(
                            new TimeValue(flushBackoffPolicy.getDelayMillis()),
                            flushBackoffPolicy.getMaxRetryCount());
            }
        } else {
            backoffPolicy = BackoffPolicy.noBackoff();
        }

        builder.setBackoffPolicy(backoffPolicy);
    }

}
