package com.dtstack.flink.sql.sink.elasticsearch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.util.NoOpFailureHandler;

import com.dtstack.flink.sql.metric.MetricConstant;
import com.dtstack.flink.sql.sink.elasticsearch.table.ElasticsearchTableInfo;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.List;
import java.util.Map;

/**
 * @author zihjiang
 * @date 2020/10/22 - 15:10
 */
public class MetricElasticsearchSink<T> extends ElasticsearchSinkBase<T, RestHighLevelClient> {

    protected CustomerSinkFunc customerSinkFunc;

    protected transient Meter outRecordsRate;

    protected Map userConfig;


    public MetricElasticsearchSink(Map userConfig, List transportAddresses,
                                    ElasticsearchSinkFunction elasticsearchSinkFunction,
                                    ElasticsearchTableInfo es6TableInfo) {
        super(new ExtendEsApiCallBridge(transportAddresses, es6TableInfo), userConfig, elasticsearchSinkFunction, new NoOpFailureHandler());
        this.customerSinkFunc = (CustomerSinkFunc) elasticsearchSinkFunction;
        this.userConfig = userConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        initMetric();
    }


    public void initMetric() {
        Counter counter = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_OUT);
        Counter outDirtyRecords = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_DIRTY_RECORDS_OUT);

        customerSinkFunc.setOutRecords(counter);
        customerSinkFunc.setOutDirtyRecords(outDirtyRecords);
        outRecordsRate = getRuntimeContext().getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_OUT_RATE, new MeterView(counter, 20));
    }
}
