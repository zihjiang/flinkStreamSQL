package com.dtstack.flink.sql.sink.elasticsearch;

import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import com.dtstack.flink.sql.util.DtStringUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zihjiang
 * @date 2020/10/22 - 15:10
 */
public class EsUtil {

    public static Map<String, Object> rowToJsonMap(Row row, List<String> fields, List<String> types) {
        Preconditions.checkArgument(row.getArity() == fields.size());
        Map<String,Object> jsonMap = Maps.newHashMap();
        int i = 0;
        for(; i < fields.size(); ++i) {
            String field = fields.get(i);
            String[] parts = field.split("\\.");
            Map<String, Object> currMap = jsonMap;
            for(int j = 0; j < parts.length - 1; ++j) {
                String key = parts[j];
                if(currMap.get(key) == null) {
                    HashMap<String, Object> hashMap = Maps.newHashMap();
                    currMap.put(key, hashMap);
                }
                currMap = (Map<String, Object>) currMap.get(key);
            }
            String key = parts[parts.length - 1];
            Object col = row.getField(i);
            if(col != null) {
                Object value = DtStringUtil.col2string(col, types.get(i));
                currMap.put(key, value);
            }

        }

        return jsonMap;
    }


}
