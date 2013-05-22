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
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.hybridhash.HybridHashGroupOperatorDescriptor;

public class HybridHashGroupByPOperator extends AbstractGroupByPOperator {

    protected final long inputRows;
    protected final long inputKeyCardinality;
    protected final int recordSizeInBytes;

    public HybridHashGroupByPOperator(List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> gbyList, int frameLimit,
            int tableSize, long inputRows, long inputKeyCard, int recSizeInBytes) {
        super(gbyList, frameLimit, tableSize);
        this.inputRows = inputRows;
        this.inputKeyCardinality = inputKeyCard;
        this.recordSizeInBytes = recSizeInBytes;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator#getOperatorTag()
     */
    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.HYBRID_HASH_GROUP_BY;
    }

    @Override
    public IOperatorDescriptor getHyracksGroupByOperator(IOperatorDescriptorRegistry spec, int[] inputKeys,
            int[] intermediateKeys, IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFactory[] hashFunctionFactories, IBinaryHashFunctionFamily[] hashFunctionFamilies,
            INormalizedKeyComputerFactory nkf, IAggregatorDescriptorFactory aggFactory,
            IAggregatorDescriptorFactory mergeFactory, RecordDescriptor outRecDesc) throws HyracksDataException {
        return new HybridHashGroupOperatorDescriptor(spec, inputKeys, frameLimit, inputRows, inputKeyCardinality,
                recordSizeInBytes, tableSize, comparatorFactories, hashFunctionFamilies, 0, nkf, aggFactory,
                mergeFactory, outRecDesc);
    }
}
