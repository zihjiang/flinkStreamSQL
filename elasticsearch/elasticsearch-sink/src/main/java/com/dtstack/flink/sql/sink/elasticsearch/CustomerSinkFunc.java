
package com.dtstack.flink.sql.sink.elasticsearch;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.types.Row;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author zihjiang
 * @date 2020/10/22 - 15:10
 */
public class CustomerSinkFunc implements ElasticsearchSinkFunction<Tuple2> {

    private final Logger logger = LoggerFactory.getLogger(CustomerSinkFunc.class);
    /** 用作ID的属性值连接符号 */
    private static final String ID_VALUE_SPLIT = "_";

    private String index;

    private String type;

    private List<Integer> idFieldIndexList;

    private List<String> fieldNames;

    private List<String> fieldTypes;

    private transient Counter outRecords;

    private transient Counter outDirtyRecords;

    public CustomerSinkFunc(String index, String type, List<String> fieldNames, List<String> fieldTypes, List<Integer> idFieldIndexes) {
        this.index = index;
        this.type = type;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.idFieldIndexList = idFieldIndexes;
    }

    @Override
    public void process(Tuple2 tuple2, RuntimeContext ctx, RequestIndexer indexer) {
        try {
            Tuple2<Boolean, Row> tupleTrans = tuple2;
            Boolean retract = tupleTrans.getField(0);
            Row element = tupleTrans.getField(1);
            if (!retract) {
                return;
            }

            indexer.add(createIndexRequest(element));
            outRecords.inc();
        } catch (Throwable e) {
            outDirtyRecords.inc();
            logger.error("Failed to store source data {}. ", tuple2.getField(1));
            logger.error("Failed to create index request exception. ", e);
        }
    }

    public void setOutRecords(Counter outRecords) {
        this.outRecords = outRecords;
    }

    public void setOutDirtyRecords(Counter outDirtyRecords) {
        this.outDirtyRecords = outDirtyRecords;
    }

    private IndexRequest createIndexRequest(Row element) {
        String idFieldStr = "";
        if (null != idFieldIndexList) {
            // index start at 1,
            idFieldStr = idFieldIndexList.stream()
                    .filter(index -> index > 0 && index <= element.getArity())
                    .map(index -> element.getField(index - 1).toString())
                    .collect(Collectors.joining(ID_VALUE_SPLIT));
        }

        Map<String, Object> dataMap = EsUtil.rowToJsonMap(element, fieldNames, fieldTypes);
        int length = Math.min(element.getArity(), fieldNames.size());
        for (int i = 0; i < length; i++) {
            dataMap.put(fieldNames.get(i), element.getField(i));
        }


        if (StringUtils.isEmpty(idFieldStr)) {
            return Requests.indexRequest()
                    .index(index)
                    .type(type)
                    .source(dataMap);
        }

        return Requests.indexRequest()
                .index(index)
                .type(type)
                .id(idFieldStr)
                .source(dataMap);
    }
}
