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

package org.apache.kafka.metadata.placement;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;


/**
 * The interface which a Kafka replica placement policy must implement.
 */
/*
    HINTS Kafka元数据管理模块的核心接口，用于动态决定 Kafka 分区副本在 Broker集群中中的分布策略。
 */
@InterfaceStability.Unstable
public interface ReplicaPlacer {
    /**
     * Create a new replica placement.
     * 创建新的副本放置
     *
     * @param placement     What zwe're trying to place.  分区防止的规格要求（如副本数量、分区数 ）
     * @param cluster       A description of the cluster we're trying to place in. 集群状态描述 （如 Broker列表、机架信息）
     *
     * @return              A topic assignment.
     *
     * @throws InvalidReplicationFactorException    If too many replicas were requested.
     */
    TopicAssignment place(PlacementSpec placement, ClusterDescriber cluster) throws InvalidReplicationFactorException;
}