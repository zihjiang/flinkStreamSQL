/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



package com.dtstack.flink.sql.source.kafka.table;

import com.dtstack.flink.sql.format.FormatType;
import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Reason:
 * Date: 2018/09/18
 * Company: www.dtstack.com
 * @author sishu.yss
 */

public class KafkaSourceTableInfo extends AbstractSourceTableInfo {

	public static final String BOOTSTRAPSERVERS_KEY = "bootstrapServers";

	public static final String TOPIC_KEY = "topic";

	public static final String TYPE_KEY = "type";

	public static final String GROUPID_KEY = "groupId";

	public static final String OFFSETRESET_KEY = "offsetReset";

	public static final String TOPICISPATTERN_KEY = "topicIsPattern";

	private String bootstrapServers;

	private String topic;

	private String groupId;

	//latest, earliest
	private String offsetReset = "latest";

	private String offset;

	private Boolean topicIsPattern = false;

	private String sourceDataType = FormatType.DT_NEST.name();

	private String schemaString;

	private String fieldDelimiter;

	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public String getOffsetReset() {
		return offsetReset;
	}

	public void setOffsetReset(String offsetReset) {
		if(offsetReset == null){
			return;
		}
		this.offsetReset = offsetReset;
	}

	public String getOffset() {
		return offset;
	}

	public void setOffset(String offset) {
		if (offsetReset == null) {
			return;
		}
		this.offset = offset;
	}

	public Boolean getTopicIsPattern() {
		return topicIsPattern;
	}

	public void setTopicIsPattern(Boolean topicIsPattern) {
		this.topicIsPattern = topicIsPattern;
	}

	public Map<String, String> kafkaParam = new HashMap<>();

	public void addKafkaParam(String key, String value) {
		kafkaParam.put(key, value);
	}

	public String getKafkaParam(String key) {
		return kafkaParam.get(key);
	}

	public Set<String> getKafkaParamKeys() {
		return kafkaParam.keySet();
	}

	public String getSourceDataType() {
		return sourceDataType;
	}

	public void setSourceDataType(String sourceDataType) {
		this.sourceDataType = sourceDataType;
	}

	public String getSchemaString() {
		return schemaString;
	}

	public void setSchemaString(String schemaString) {
		this.schemaString = schemaString;
	}

	public String getFieldDelimiter() {
		return fieldDelimiter;
	}

	public void setFieldDelimiter(String fieldDelimiter) {
		this.fieldDelimiter = fieldDelimiter;
	}

	@Override
	public boolean check() {
		Preconditions.checkNotNull(getType(), "kafka of type is required");
		Preconditions.checkNotNull(bootstrapServers, "kafka of bootstrapServers is required");
		Preconditions.checkNotNull(topic, "kafka of topic is required");
		return false;
	}
}
