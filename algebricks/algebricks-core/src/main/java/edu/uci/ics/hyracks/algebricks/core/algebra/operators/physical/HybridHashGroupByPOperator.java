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

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;

public class HybridHashGroupByPOperator extends AbstractPhysicalOperator {

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator#getOperatorTag()
     */
    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.HYBRID_HASH_GROUP_BY;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator#getRequiredPropertiesForChildren(edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator, edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector)
     */
    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator#computeDeliveredProperties(edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator, edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext)
     */
    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator#contributeRuntimeOperator(edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder, edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext, edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator, edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema, edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema[], edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema)
     */
    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator#isMicroOperator()
     */
    @Override
    public boolean isMicroOperator() {
        return false;
    }

}
