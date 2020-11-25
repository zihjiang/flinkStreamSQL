
package com.dtstack.flink.sql.sink.elasticsearch.table;

import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * @author zihjiang
 * @date 2020/10/22 - 15:10
 */
public class ElasticsearchSinkParser extends AbstractTableParser {

    private static final String KEY_ES_ADDRESS = "address";

    private static final String KEY_ES_CLUSTER = "cluster";

    private static final String KEY_ES_INDEX = "index";

    private static final String KEY_ES_TYPE = "estype";

    private static final String KEY_ES_ID_FIELD_INDEX_LIST = "id";

    private static final String KEY_ES_AUTHMESH = "authMesh";

    private static final String KEY_ES_USERNAME = "userName";

    private static final String KEY_ES_PASSWORD = "password";

    private static final String KEY_TRUE = "true";

    @Override
    protected boolean fieldNameNeedsUpperCase() {
        return false;
    }

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        ElasticsearchTableInfo elasticsearchTableInfo = new ElasticsearchTableInfo();
        elasticsearchTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, elasticsearchTableInfo);
        elasticsearchTableInfo.setAddress((String) props.get(KEY_ES_ADDRESS.toLowerCase()));
        elasticsearchTableInfo.setClusterName((String) props.get(KEY_ES_CLUSTER.toLowerCase()));
        elasticsearchTableInfo.setId((String) props.get(KEY_ES_ID_FIELD_INDEX_LIST.toLowerCase()));
        elasticsearchTableInfo.setIndex((String) props.get(KEY_ES_INDEX.toLowerCase()));
        elasticsearchTableInfo.setEsType((String) props.get(KEY_ES_TYPE.toLowerCase()));

        String authMeshStr = (String) props.get(KEY_ES_AUTHMESH.toLowerCase());
        if (authMeshStr != null && StringUtils.equalsIgnoreCase(KEY_TRUE, authMeshStr)) {
            elasticsearchTableInfo.setAuthMesh(MathUtil.getBoolean(authMeshStr));
            elasticsearchTableInfo.setUserName(MathUtil.getString(props.get(KEY_ES_USERNAME.toLowerCase())));
            elasticsearchTableInfo.setPassword(MathUtil.getString(props.get(KEY_ES_PASSWORD.toLowerCase())));
        }
        elasticsearchTableInfo.check();
        return elasticsearchTableInfo;
    }
}
