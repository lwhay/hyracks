/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.tests.benchmarking;

import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DummyInOutOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DummyInputOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DummyInputSinkOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DummyOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNRangePartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNReplicatingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;

/**
 * @author jarodwen
 *
 */
public class DummiesWorkflowTests extends AbstractIntegrationTest {
    
    final int chainLength = 5;
    final int connectorType = 4;
    
    @Test
    public void dummyOperatorTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // Dummy operator

        DummyOperatorDescriptor dummy = new DummyOperatorDescriptor(spec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dummy, NC2_ID, NC1_ID);

        spec.addRoot(dummy);
        
        runTest(spec);
    }
    
    @Test
    public void dummyInOutOperatorTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // Dummy operator

        DummyInputOperatorDescriptor dummyIn = new DummyInputOperatorDescriptor(spec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dummyIn, NC2_ID, NC1_ID);
        
        DummyInputSinkOperatorDescriptor dummySink = new DummyInputSinkOperatorDescriptor(spec);
        
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dummySink, NC2_ID, NC1_ID);

        IConnectorDescriptor conn = new OneToOneConnectorDescriptor(spec);
        
        spec.connect(conn, dummyIn, 0, dummySink, 0);
        
        spec.addRoot(dummySink);
        
        runTest(spec);
    }
    
    @Test
    public void dummyInChainOutOperatorTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // Dummy operator

        DummyInputOperatorDescriptor dummyIn = new DummyInputOperatorDescriptor(spec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dummyIn, NC2_ID, NC1_ID);
        
        AbstractSingleActivityOperatorDescriptor opter = dummyIn;
        
        for(int i = 0; i < chainLength - 2; i++){
            DummyInOutOperatorDescriptor dummyChain = new DummyInOutOperatorDescriptor(spec);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dummyChain, NC2_ID, NC1_ID);
            IConnectorDescriptor connChain;
            switch (connectorType) {
                case 0:
                    connChain = new OneToOneConnectorDescriptor(spec);
                    break;
                case 1:
                    connChain = new MToNHashPartitioningConnectorDescriptor(spec, new FieldHashPartitionComputerFactory(new int[]{}, new IBinaryHashFunctionFactory[] {}));
                    break;
                case 2:
                    connChain = new MToNRangePartitioningConnectorDescriptor(spec, 0, null);
                    break;
                case 3:
                    connChain = new MToNHashPartitioningMergingConnectorDescriptor(spec, new FieldHashPartitionComputerFactory(new int[]{}, new IBinaryHashFunctionFactory[] {}), new int[]{}, new IBinaryComparatorFactory[]{});
                    break;
                case 4:
                    connChain = new MToNReplicatingConnectorDescriptor(spec);
                    break;
                default:
                    connChain = new OneToOneConnectorDescriptor(spec);
                    break;
            }
            
            spec.connect(connChain, opter, 0, dummyChain, 0);
            opter = dummyChain;
        }
        
        DummyInputSinkOperatorDescriptor dummySink = new DummyInputSinkOperatorDescriptor(spec);
        
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dummySink, NC2_ID, NC1_ID);

        IConnectorDescriptor conn = new OneToOneConnectorDescriptor(spec);
        
        spec.connect(conn, opter, 0, dummySink, 0);
        
        spec.addRoot(dummySink);
        
        runTest(spec);
    }
}
