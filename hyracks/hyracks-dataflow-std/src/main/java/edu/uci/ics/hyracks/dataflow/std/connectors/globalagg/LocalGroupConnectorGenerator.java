/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.connectors.globalagg;

import java.util.BitSet;

import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.HashtableLocalityMap;
import edu.uci.ics.hyracks.dataflow.std.connectors.LocalityAwareMToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.LocalityAwareMToNPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.PartitionNodeMap;
import edu.uci.ics.hyracks.dataflow.std.group.global.costmodels.CostVector;
import edu.uci.ics.hyracks.dataflow.std.group.global.costmodels.DatasetStats;
import edu.uci.ics.hyracks.dataflow.std.group.global.costmodels.GrouperProperty;
import edu.uci.ics.hyracks.dataflow.std.group.global.costmodels.GrouperProperty.Property;
import edu.uci.ics.hyracks.dataflow.std.group.global.data.HashFunctionFamilyFactoryAdapter;

public class LocalGroupConnectorGenerator {

    public enum GrouperConnector {
        HASH_CONN,
        HASH_MERGE_CONN;

        static final GrouperProperty HASH_CONN_PROP = new GrouperProperty();
        static final GrouperProperty HASH_MERGE_CONN_PROP = new GrouperProperty(Property.SORTED);

        public GrouperProperty getRequiredProperty() throws HyracksDataException {
            switch (this) {
                case HASH_CONN:
                    return HASH_CONN_PROP;
                case HASH_MERGE_CONN:
                    return HASH_MERGE_CONN_PROP;
            }
            throw new HyracksDataException("Unsupported connector: " + this.name());
        }

        public void computeOutputProperty(PartitionNodeMap nodeMap, GrouperProperty prop) throws HyracksDataException {
            switch (this) {
                case HASH_CONN:
                    if (nodeMap.isFullyPartitionNodeMap()) {
                        prop.setProperty(Property.GLOBALLY_PARTITIONED);
                    }
                    break;
                case HASH_MERGE_CONN:
                    prop.setProperty(Property.SORTED);
                    break;
            }
        }

        public void computeCostVector(CostVector costVect, DatasetStats outputStat, int recordSize, int frameSize,
                PartitionNodeMap nodeMap) {
            switch (this) {
                case HASH_MERGE_CONN:
                    // need to compute the merging cost
                    costVect.updateCpu(outputStat.getRecordCount() * Math.log(nodeMap.getFromNodes()));
                case HASH_CONN:
                    if (!nodeMap.isOneToOnePartitionNodeMap()) {
                        // need hash partition: the cpu cost for hashing
                        costVect.updateCpu(outputStat.getRecordCount());
                    }
                    break;
            }
        }
    }

    public static IConnectorDescriptor generateConnector(JobSpecification spec, GrouperConnector connectorType,
            int[] keyFields, IBinaryHashFunctionFactory[] hashFunctionFactories,
            IBinaryComparatorFactory[] comparatorFactories, BitSet nodeMap, int levelSeed) throws HyracksDataException {
        switch (connectorType) {
            case HASH_CONN:
                return new LocalityAwareMToNPartitioningConnectorDescriptor(spec,
                        new FieldHashPartitionComputerFactory(keyFields,
                                new IBinaryHashFunctionFactory[] { HashFunctionFamilyFactoryAdapter
                                        .getFunctionFactoryFromFunctionFamily(
                                                MurmurHash3BinaryHashFunctionFamily.INSTANCE, levelSeed) }),
                        new HashtableLocalityMap(nodeMap));
            case HASH_MERGE_CONN:
                return new LocalityAwareMToNPartitioningMergingConnectorDescriptor(spec,
                        new FieldHashPartitionComputerFactory(keyFields,
                                new IBinaryHashFunctionFactory[] { HashFunctionFamilyFactoryAdapter
                                        .getFunctionFactoryFromFunctionFamily(
                                                MurmurHash3BinaryHashFunctionFamily.INSTANCE, levelSeed) }),
                        new HashtableLocalityMap(nodeMap), keyFields, comparatorFactories);
        }
        throw new HyracksDataException("Not supported connector type: " + connectorType.name());
    }
}
