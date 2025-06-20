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
package org.apache.kafka.server.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.apache.kafka.coordinator.share.ShareCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.AddPartitionsToTxnConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.MetadataLogConfig;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.metrics.MetricConfigs;
import org.apache.kafka.server.util.Csv;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.storage.internals.log.LogConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * During moving {@link kafka.server.KafkaConfig} out of core AbstractKafkaConfig will be the future KafkaConfig
 * so any new getters, or updates to `CONFIG_DEF` will be defined here.
 * Any code depends on kafka.server.KafkaConfig will keep for using kafka.server.KafkaConfig for the time being until we move it out of core
 * For more details check KAFKA-15853
 */
public abstract class AbstractKafkaConfig extends AbstractConfig {
    public static final ConfigDef CONFIG_DEF = Utils.mergeConfigs(List.of(
        RemoteLogManagerConfig.configDef(),
        ServerConfigs.CONFIG_DEF,
        KRaftConfigs.CONFIG_DEF,
        MetadataLogConfig.CONFIG_DEF,
        SocketServerConfigs.CONFIG_DEF,
        ReplicationConfigs.CONFIG_DEF,
        GroupCoordinatorConfig.CONFIG_DEF,
        CleanerConfig.CONFIG_DEF,
        LogConfig.SERVER_CONFIG_DEF,
        ShareGroupConfig.CONFIG_DEF,
        ShareCoordinatorConfig.CONFIG_DEF,
        TransactionLogConfig.CONFIG_DEF,
        TransactionStateManagerConfig.CONFIG_DEF,
        QuorumConfig.CONFIG_DEF,
        MetricConfigs.CONFIG_DEF,
        QuotaConfig.CONFIG_DEF,
        BrokerSecurityConfigs.CONFIG_DEF,
        DelegationTokenManagerConfigs.CONFIG_DEF,
        AddPartitionsToTxnConfig.CONFIG_DEF
    ));

    public AbstractKafkaConfig(ConfigDef definition, Map<?, ?> originals, Map<String, ?> configProviderProps, boolean doLog) {
        super(definition, originals, configProviderProps, doLog);
    }

    public List<String> logDirs() {
        return Csv.parseCsvList(Optional.ofNullable(getString(ServerLogConfigs.LOG_DIRS_CONFIG)).orElse(getString(ServerLogConfigs.LOG_DIR_CONFIG)));
    }

    public int numIoThreads() {
        return getInt(ServerConfigs.NUM_IO_THREADS_CONFIG);
    }

    public int numReplicaFetchers() {
        return getInt(ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG);
    }

    public int numRecoveryThreadsPerDataDir() {
        return getInt(ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG);
    }

    public int backgroundThreads() {
        return getInt(ServerConfigs.BACKGROUND_THREADS_CONFIG);
    }
}
