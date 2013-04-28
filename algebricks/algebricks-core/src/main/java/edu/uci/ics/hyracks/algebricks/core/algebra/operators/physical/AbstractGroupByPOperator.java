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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.ListSet;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.LocalGroupingProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;

public abstract class AbstractGroupByPOperator extends AbstractPhysicalOperator {

    protected final int tableSize;
    protected final int frameLimit;

    private List<LogicalVariable> columnSet = new ArrayList<LogicalVariable>();

    public AbstractGroupByPOperator(List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> gbyList, int frameLimit,
            int tableSize) {
        this.tableSize = tableSize;
        this.frameLimit = frameLimit;
        computeColumnSet(gbyList);
    }

    public void computeColumnSet(List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> gbyList) {
        columnSet.clear();
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gbyList) {
            ILogicalExpression expr = p.second.getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression v = (VariableReferenceExpression) expr;
                columnSet.add(v.getVariableReference());
            }
        }
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator#getRequiredPropertiesForChildren(edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator, edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector)
     */
    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) op;
        if (aop.getExecutionMode() == ExecutionMode.PARTITIONED) {
            StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
            pv[0] = new StructuralPropertiesVector(new UnorderedPartitionedProperty(new ListSet<LogicalVariable>(
                    columnSet), null), null);
            return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
        } else {
            return emptyUnaryRequirements();
        }
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator#computeDeliveredProperties(edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator, edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext)
     */
    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        List<ILocalStructuralProperty> propsLocal = new LinkedList<ILocalStructuralProperty>();

        GroupByOperator gOp = (GroupByOperator) op;
        Set<LogicalVariable> columnSet = new ListSet<LogicalVariable>();

        if (!columnSet.isEmpty()) {
            propsLocal.add(new LocalGroupingProperty(columnSet));
        }
        for (ILogicalPlan p : gOp.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                ILogicalOperator rOp = r.getValue();
                propsLocal.addAll(rOp.getDeliveredPhysicalProperties().getLocalProperties());
            }
        }

        ILogicalOperator op2 = op.getInputs().get(0).getValue();
        IPhysicalPropertiesVector childProp = op2.getDeliveredPhysicalProperties();
        deliveredProperties = new StructuralPropertiesVector(childProp.getPartitioningProperty(), propsLocal);
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator#isMicroOperator()
     */
    @Override
    public boolean isMicroOperator() {
        return false;
    }

}