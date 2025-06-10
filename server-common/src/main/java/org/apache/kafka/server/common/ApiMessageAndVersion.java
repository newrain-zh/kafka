/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.server.common;

import org.apache.kafka.common.protocol.ApiMessage;

import java.util.Objects;

/**
 * An ApiMessage and an associated version.
 */
/*
    HINTS Kafka元数据管理中的协议消息版本封装器，其核心作用是为不同版本的元数据变更提供同一的结构化表示。
 */
public class ApiMessageAndVersion {
    private final ApiMessage message; // 具体的消息记录
    private final short version; // 表示消息对应的协议版本

    public ApiMessageAndVersion(ApiMessage message, short version) {
        this.message = message;
        this.version = version;
    }

    public ApiMessage message() {
        return message;
    }

    public short version() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ApiMessageAndVersion that = (ApiMessageAndVersion) o;
        return version == that.version &&
            Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, version);
    }

    @Override
    public String toString() {
        return "ApiMessageAndVersion(" + message + " at version " + version + ")";
    }
}