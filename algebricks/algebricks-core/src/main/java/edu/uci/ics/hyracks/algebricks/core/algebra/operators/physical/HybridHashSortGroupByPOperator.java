/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.hashsort.HybridHashSortGroupOperatorDescriptor;

public class HybridHashSortGroupByPOperator extends AbstractGroupByPOperator {

    public HybridHashSortGroupByPOperator(List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> gbyList,
            int frameLimit, int tableSize) {
        super(gbyList, frameLimit, tableSize);
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator#getOperatorTag()
     */
    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.HASH_SORT_GROUP_BY;
    }

    @Override
    public IOperatorDescriptor getHyracksGroupByOperator(IOperatorDescriptorRegistry spec, int[] inputKeys,
            int[] intermediateKeys, IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFactory[] hashFunctionFactories, IBinaryHashFunctionFamily[] hashFunctionFamilies,
            INormalizedKeyComputerFactory nkf, IAggregatorDescriptorFactory aggFactory,
            IAggregatorDescriptorFactory mergeFactory, RecordDescriptor outRecDesc) throws HyracksDataException {
        return new HybridHashSortGroupOperatorDescriptor(spec, inputKeys, frameLimit, tableSize, comparatorFactories,
                new FieldHashPartitionComputerFactory(inputKeys, hashFunctionFactories),
                new FieldHashPartitionComputerFactory(intermediateKeys, hashFunctionFactories), nkf, aggFactory,
                mergeFactory, outRecDesc, false);
    }
}
