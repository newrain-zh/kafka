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

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.metadata.placement.StripedReplicaPlacer.BrokerList;
import org.apache.kafka.metadata.placement.StripedReplicaPlacer.RackList;
import org.apache.kafka.server.util.MockRandom;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.metadata.placement.PartitionAssignmentTest.partitionAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Timeout(value = 40)
public class StripedReplicaPlacerTest {
    /**
     * Test that the BrokerList class works as expected.
     */
    @Test
    public void testBrokerList() {
        assertEquals(0, BrokerList.EMPTY.size());
        assertEquals(-1, BrokerList.EMPTY.next(1));
        BrokerList brokers = new BrokerList().add(0).add(1).add(2).add(3);
        assertEquals(4, brokers.size());
        assertEquals(0, brokers.next(0));
        assertEquals(1, brokers.next(0));
        assertEquals(2, brokers.next(0));
        assertEquals(3, brokers.next(0));
        assertEquals(-1, brokers.next(0));
        assertEquals(-1, brokers.next(0));
        assertEquals(1, brokers.next(1));
        assertEquals(2, brokers.next(1));
        assertEquals(3, brokers.next(1));
        assertEquals(0, brokers.next(1));
        assertEquals(-1, brokers.next(1));
    }

    /**
     * Test that we perform striped replica placement as expected, and don't use the
     * fenced replica if we don't have to.
     * 测试我们是否按预期执行条带化副本放置，如果不需要，请不要使用受防护的副本。
     */
    @Test
    public void testAvoidFencedReplicaIfPossibleOnSingleRack() {
        MockRandom random = new MockRandom();
        RackList rackList = new RackList(random, List.of(
            new UsableBroker(3, Optional.empty(), false),
            new UsableBroker(1, Optional.empty(), true),
            new UsableBroker(0, Optional.empty(), false),
            new UsableBroker(4, Optional.empty(), false),
            new UsableBroker(2, Optional.empty(), false)).iterator());
        assertEquals(5, rackList.numTotalBrokers());
        assertEquals(4, rackList.numUnfencedBrokers());
        assertEquals(List.of(Optional.empty()), rackList.rackNames());
        assertThrows(InvalidReplicationFactorException.class, () -> rackList.place(0));
        assertThrows(InvalidReplicationFactorException.class, () -> rackList.place(-1));
        assertEquals(List.of(3, 4, 0, 2), rackList.place(4));
        assertEquals(List.of(4, 0, 2, 3), rackList.place(4));
        assertEquals(List.of(0, 2, 3, 4), rackList.place(4));
        assertEquals(List.of(2, 3, 4, 0), rackList.place(4));
        assertEquals(List.of(0, 4, 3, 2), rackList.place(4));
    }

    private TopicAssignment place(
        ReplicaPlacer placer,
        int startPartition,
        int numPartitions,
        short replicationFactor,
        List<UsableBroker> brokers
    ) {
        PlacementSpec placementSpec = new PlacementSpec(startPartition,
            numPartitions,
            replicationFactor);
        return placer.place(placementSpec, new ClusterDescriber() {
            @Override
            public Iterator<UsableBroker> usableBrokers() {
                return brokers.iterator();
            }

            @Override
            public Uuid defaultDir(int brokerId) {
                return DirectoryId.MIGRATING;
            }
        });
    }

    /**
     * Test that we perform striped replica placement as expected for a multi-partition topic
     * on a single unfenced broker
     */
    @Test
    public void testMultiPartitionTopicPlacementOnSingleUnfencedBroker() {
        MockRandom random = new MockRandom();
        StripedReplicaPlacer placer = new StripedReplicaPlacer(random);
        assertEquals(new TopicAssignment(List.of(partitionAssignment(List.of(0)),
                partitionAssignment(List.of(0)),
                partitionAssignment(List.of(0)))),
                place(placer, 0, 3, (short) 1, List.of(
                        new UsableBroker(0, Optional.empty(), false),
                        new UsableBroker(1, Optional.empty(), true))));
    }

    /**
     * Test that we will place on the fenced replica if we need to.
     */
    @Test
    public void testPlacementOnFencedReplicaOnSingleRack() {
        MockRandom random = new MockRandom();
        RackList rackList = new RackList(random, List.of(
            new UsableBroker(3, Optional.empty(), false),
            new UsableBroker(1, Optional.empty(), true),
            new UsableBroker(2, Optional.empty(), false)).iterator());
        assertEquals(3, rackList.numTotalBrokers());
        assertEquals(2, rackList.numUnfencedBrokers());
        assertEquals(List.of(Optional.empty()), rackList.rackNames());
        assertEquals(List.of(3, 2, 1), rackList.place(3));
        assertEquals(List.of(2, 3, 1), rackList.place(3));
        assertEquals(List.of(3, 2, 1), rackList.place(3));
        assertEquals(List.of(2, 3, 1), rackList.place(3));
    }

    @Test
    public void testRackListWithMultipleRacks() {
        MockRandom random = new MockRandom();
        RackList rackList = new RackList(random, List.of(
            new UsableBroker(11, Optional.of("1"), false),
            new UsableBroker(10, Optional.of("1"), false),
            new UsableBroker(30, Optional.of("3"), false),
            new UsableBroker(31, Optional.of("3"), false),
            new UsableBroker(21, Optional.of("2"), false),
            new UsableBroker(20, Optional.of("2"), true)).iterator());
        assertEquals(6, rackList.numTotalBrokers());
        assertEquals(5, rackList.numUnfencedBrokers());
        assertEquals(List.of(Optional.of("1"), Optional.of("2"), Optional.of("3")), rackList.rackNames());
        assertEquals(List.of(11, 21, 31, 10), rackList.place(4));
        assertEquals(List.of(21, 30, 10, 20), rackList.place(4));
        assertEquals(List.of(31, 11, 21, 30), rackList.place(4));
    }

    @Test
    public void testRackListWithInvalidRacks() {
        MockRandom random = new MockRandom();
        RackList rackList = new RackList(random, List.of(
            new UsableBroker(11, Optional.of("1"), false),
            new UsableBroker(10, Optional.of("1"), false),
            new UsableBroker(30, Optional.of("3"), true),
            new UsableBroker(31, Optional.of("3"), true),
            new UsableBroker(20, Optional.of("2"), true),
            new UsableBroker(21, Optional.of("2"), true),
            new UsableBroker(41, Optional.of("4"), false),
            new UsableBroker(40, Optional.of("4"), true)).iterator());
        assertEquals(8, rackList.numTotalBrokers());
        assertEquals(3, rackList.numUnfencedBrokers());
        assertEquals(List.of(Optional.of("1"),
            Optional.of("2"),
            Optional.of("3"),
            Optional.of("4")), rackList.rackNames());
        assertEquals(List.of(41, 11, 21, 30), rackList.place(4));
        assertEquals(List.of(10, 20, 31, 41), rackList.place(4));
        assertEquals(List.of(41, 21, 30, 11), rackList.place(4));
    }

    @Test
    public void testAllBrokersFenced() {
        MockRandom random = new MockRandom();
        StripedReplicaPlacer placer = new StripedReplicaPlacer(random);
        assertEquals("All brokers are currently fenced.",
            assertThrows(InvalidReplicationFactorException.class,
                () -> place(placer, 0, 1, (short) 1, List.of(
                    new UsableBroker(11, Optional.of("1"), true),
                    new UsableBroker(10, Optional.of("1"), true)))).getMessage());
    }

    @Test
    public void testNotEnoughBrokers() {
        MockRandom random = new MockRandom();
        StripedReplicaPlacer placer = new StripedReplicaPlacer(random);
        assertEquals("The target replication factor of 3 cannot be reached because only " +
            "2 broker(s) are registered.",
            assertThrows(InvalidReplicationFactorException.class,
                () -> place(placer, 0, 1, (short) 3, List.of(
                    new UsableBroker(11, Optional.of("1"), false),
                    new UsableBroker(10, Optional.of("1"), false)))).getMessage());
    }

    @Test
    public void testNonPositiveReplicationFactor() {
        MockRandom random = new MockRandom();
        StripedReplicaPlacer placer = new StripedReplicaPlacer(random);
        assertEquals("Invalid replication factor 0: the replication factor must be positive.",
                assertThrows(InvalidReplicationFactorException.class,
                        () -> place(placer, 0, 1, (short) 0, List.of(
                                new UsableBroker(11, Optional.of("1"), false),
                                new UsableBroker(10, Optional.of("1"), false)))).getMessage());
    }

    @Test
    public void testSuccessfulPlacement() {
        MockRandom random = new MockRandom();
        StripedReplicaPlacer placer = new StripedReplicaPlacer(random);
        assertEquals(new TopicAssignment(List.of(partitionAssignment(List.of(2, 3, 0)),
                partitionAssignment(List.of(3, 0, 1)),
                partitionAssignment(List.of(0, 1, 2)),
                partitionAssignment(List.of(1, 2, 3)),
                partitionAssignment(List.of(1, 0, 2)))),
            place(placer, 0, 5, (short) 3, List.of(
                new UsableBroker(0, Optional.empty(), false),
                new UsableBroker(3, Optional.empty(), false),
                new UsableBroker(2, Optional.empty(), false),
                new UsableBroker(1, Optional.empty(), false))));
    }

    @Test
    public void testEvenDistribution() {
        MockRandom random = new MockRandom();
        StripedReplicaPlacer placer = new StripedReplicaPlacer(random);
        TopicAssignment topicAssignment = place(placer, 0, 200, (short) 2, List.of(
            new UsableBroker(0, Optional.empty(), false),
            new UsableBroker(1, Optional.empty(), false),
            new UsableBroker(2, Optional.empty(), false),
            new UsableBroker(3, Optional.empty(), false)));
        Map<List<Integer>, Integer> counts = new HashMap<>();
        for (PartitionAssignment partitionAssignment : topicAssignment.assignments()) {
            counts.put(partitionAssignment.replicas(), counts.getOrDefault(partitionAssignment.replicas(), 0) + 1);
        }
        assertEquals(14, counts.get(List.of(0, 1)));
        assertEquals(22, counts.get(List.of(0, 2)));
        assertEquals(14, counts.get(List.of(0, 3)));
        assertEquals(17, counts.get(List.of(1, 0)));
        assertEquals(17, counts.get(List.of(1, 2)));
        assertEquals(16, counts.get(List.of(1, 3)));
        assertEquals(13, counts.get(List.of(2, 0)));
        assertEquals(17, counts.get(List.of(2, 1)));
        assertEquals(20, counts.get(List.of(2, 3)));
        assertEquals(20, counts.get(List.of(3, 0)));
        assertEquals(19, counts.get(List.of(3, 1)));
        assertEquals(11, counts.get(List.of(3, 2)));
    }

    @Test
    public void testRackListAllBrokersFenced() {
        // ensure we can place N replicas on a rack when the rack has less than N brokers
        MockRandom random = new MockRandom();
        RackList rackList = new RackList(random, List.of(
                new UsableBroker(0, Optional.empty(), true),
                new UsableBroker(1, Optional.empty(), true),
                new UsableBroker(2, Optional.empty(), true)).iterator());
        assertEquals(3, rackList.numTotalBrokers());
        assertEquals(0, rackList.numUnfencedBrokers());
        assertEquals(List.of(Optional.empty()), rackList.rackNames());
        assertEquals("All brokers are currently fenced.",
                assertThrows(InvalidReplicationFactorException.class,
                        () -> rackList.place(3)).getMessage());
    }

    @Test
    public void testRackListNotEnoughBrokers() {
        MockRandom random = new MockRandom();
        RackList rackList = new RackList(random, List.of(
                new UsableBroker(11, Optional.of("1"), false),
                new UsableBroker(10, Optional.of("1"), false)).iterator());
        assertEquals("The target replication factor of 3 cannot be reached because only " +
                        "2 broker(s) are registered.",
                assertThrows(InvalidReplicationFactorException.class,
                        () -> rackList.place(3)).getMessage());
    }

    @Test
    public void testRackListNonPositiveReplicationFactor() {
        MockRandom random = new MockRandom();
        RackList rackList = new RackList(random, List.of(
                new UsableBroker(11, Optional.of("1"), false),
                new UsableBroker(10, Optional.of("1"), false)).iterator());
        assertEquals("Invalid replication factor -1: the replication factor must be positive.",
                assertThrows(InvalidReplicationFactorException.class,
                        () -> rackList.place(-1)).getMessage());
    }
}