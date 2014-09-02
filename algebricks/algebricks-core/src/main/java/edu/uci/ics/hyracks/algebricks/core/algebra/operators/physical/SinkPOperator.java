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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.SinkRuntimeFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.misc.SinkOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.union.UnionAllOperatorDescriptor;

public class SinkPOperator extends AbstractPhysicalOperator {

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.SINK;
    }

    @Override
    public boolean isMicroOperator() {
//        return true;
        return false;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
		AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs()
				.get(0).getValue();
		List<ILocalStructuralProperty> propsLocal = new ArrayList<ILocalStructuralProperty>();
		IPhysicalPropertiesVector childsProperties = op2.getPhysicalOperator()
				.getDeliveredProperties();
		if (childsProperties.getLocalProperties() != null) {
			propsLocal.addAll(childsProperties.getLocalProperties());
		}

        if (op.getInputs().size() > 1) {
        	for (int i=1; i<op.getInputs().size(); i++) {
                op2 = (AbstractLogicalOperator) op.getInputs().get(i).getValue();
                IPhysicalPropertiesVector childsProperties1 = op2.getPhysicalOperator().getDeliveredProperties();
                if (childsProperties1.getLocalProperties() != null) {
                    propsLocal.addAll(childsProperties1.getLocalProperties());
                }
        	}
        }

        deliveredProperties = new StructuralPropertiesVector(childsProperties.getPartitioningProperty(), propsLocal);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
      return emptyUnaryRequirements(op.getInputs().size());
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
    	
    	IOperatorDescriptorRegistry spec = builder.getJobSpec();
        RecordDescriptor recordDescriptor = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema, context);

        SinkOperatorDescriptor opDesc = new SinkOperatorDescriptor(spec, op.getInputs().size());
        contributeOpDesc(builder, (AbstractLogicalOperator) op, opDesc);
        
        List<ILogicalOperator> src = new ArrayList<ILogicalOperator>();
        
        for (int i=0; i< op.getInputs().size(); i++) {
            src.add(op.getInputs().get(i).getValue());
            builder.contributeGraphEdge(src.get(i), 0, op, i);
        }
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return false;
    }
}
