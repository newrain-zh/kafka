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

import java.util.List;
import java.util.Objects;

/**
 * The topic assignment.
 *
 * This class is immutable. It's internal state does not change.
 */
public class TopicAssignment {

    //  每个分区的副本分配方案
    // 数组的索引对应分区的编号
    private final List<PartitionAssignment> assignments; // 存储所有分区的副本分配方案

    public TopicAssignment(List<PartitionAssignment> assignments) {
        this.assignments = List.copyOf(assignments);
    }

    /**
     * 每个分区的副本分配，其中列表中的索引对应于不同的分区。
     * @return The replica assignment for each partition, where the index in the list corresponds to different partition.
     */
    public List<PartitionAssignment> assignments() {
        return assignments;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TopicAssignment other)) return false;
        return assignments.equals(other.assignments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(assignments);
    }

    @Override
    public String toString() {
        return "TopicAssignment" +
            "(assignments=" + assignments +
            ")";
    }
}