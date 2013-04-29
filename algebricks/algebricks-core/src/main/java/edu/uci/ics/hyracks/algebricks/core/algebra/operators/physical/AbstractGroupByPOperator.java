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
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IPartialAggregationTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
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
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.OperatorSchemaImpl;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.aggreg.SerializableAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

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

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        List<LogicalVariable> gbyCols = getGbyColumns();
        int keys[] = JobGenHelper.variablesToFieldIndexes(gbyCols, inputSchemas[0]);
        GroupByOperator gby = (GroupByOperator) op;
        int numFds = gby.getDecorList().size();
        int fdColumns[] = new int[numFds];
        int j = 0;
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gby.getDecorList()) {
            ILogicalExpression expr = p.second.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                throw new AlgebricksException("Hash-sort group-by expects variable references.");
            }
            VariableReferenceExpression v = (VariableReferenceExpression) expr;
            LogicalVariable decor = v.getVariableReference();
            fdColumns[j++] = inputSchemas[0].findVariable(decor);
        }

        if (gby.getNestedPlans().size() != 1) {
            throw new AlgebricksException(
                    "Hash-sort group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
        ILogicalPlan p0 = gby.getNestedPlans().get(0);
        if (p0.getRoots().size() != 1) {
            throw new AlgebricksException(
                    "Hash-sort group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
        Mutable<ILogicalOperator> r0 = p0.getRoots().get(0);
        AggregateOperator aggOp = (AggregateOperator) r0.getValue();

        IPartialAggregationTypeComputer partialAggregationTypeComputer = context.getPartialAggregationTypeComputer();
        List<Object> intermediateTypes = new ArrayList<Object>();
        int n = aggOp.getExpressions().size();
        ICopySerializableAggregateFunctionFactory[] aff = new ICopySerializableAggregateFunctionFactory[n];
        int i = 0;
        IExpressionRuntimeProvider expressionRuntimeProvider = context.getExpressionRuntimeProvider();
        IVariableTypeEnvironment aggOpInputEnv = context.getTypeEnvironment(aggOp.getInputs().get(0).getValue());
        IVariableTypeEnvironment outputEnv = context.getTypeEnvironment(op);
        for (Mutable<ILogicalExpression> exprRef : aggOp.getExpressions()) {
            AggregateFunctionCallExpression aggFun = (AggregateFunctionCallExpression) exprRef.getValue();
            aff[i++] = expressionRuntimeProvider.createSerializableAggregateFunctionFactory(aggFun, aggOpInputEnv,
                    inputSchemas, context);
            intermediateTypes.add(partialAggregationTypeComputer.getType(aggFun, aggOpInputEnv,
                    context.getMetadataProvider()));
        }

        int[] keyAndDecFields = new int[keys.length + fdColumns.length];
        for (i = 0; i < keys.length; ++i) {
            keyAndDecFields[i] = keys[i];
        }
        for (i = 0; i < fdColumns.length; i++) {
            keyAndDecFields[keys.length + i] = fdColumns[i];
        }

        // internal keys
        int[] intermediateKeys = new int[keys.length];
        for (i = 0; i < intermediateKeys.length; i++) {
            intermediateKeys[i] = i;
        }

        List<LogicalVariable> keyAndDecVariables = new ArrayList<LogicalVariable>();
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gby.getGroupByList())
            keyAndDecVariables.add(p.first);
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gby.getDecorList())
            keyAndDecVariables.add(GroupByOperator.getDecorVariable(p));

        for (LogicalVariable var : keyAndDecVariables)
            aggOpInputEnv.setVarType(var, outputEnv.getVarType(var));

        compileSubplans(inputSchemas[0], gby, propagatedSchema, context);
        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        IBinaryComparatorFactory[] comparatorFactories = JobGenHelper.variablesToAscBinaryComparatorFactories(gbyCols,
                aggOpInputEnv, context);
        RecordDescriptor recordDescriptor = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op),
                propagatedSchema, context);
        IBinaryHashFunctionFactory[] hashFunctionFactories = JobGenHelper.variablesToBinaryHashFunctionFactories(
                gbyCols, aggOpInputEnv, context);
        IBinaryHashFunctionFamily[] hashFunctionFamilies = JobGenHelper.variablesToBinaryHashFunctionFamilies(gbyCols,
                aggOpInputEnv, context);

        ICopySerializableAggregateFunctionFactory[] merges = new ICopySerializableAggregateFunctionFactory[n];
        List<LogicalVariable> usedVars = new ArrayList<LogicalVariable>();
        IOperatorSchema[] localInputSchemas = new IOperatorSchema[1];
        localInputSchemas[0] = new OperatorSchemaImpl();
        for (i = 0; i < n; i++) {
            AggregateFunctionCallExpression aggFun = (AggregateFunctionCallExpression) aggOp.getMergeExpressions()
                    .get(i).getValue();
            aggFun.getUsedVariables(usedVars);
        }
        i = 0;
        for (Object type : intermediateTypes) {
            aggOpInputEnv.setVarType(usedVars.get(i++), type);
        }
        for (LogicalVariable keyVar : keyAndDecVariables)
            localInputSchemas[0].addVariable(keyVar);
        for (LogicalVariable usedVar : usedVars)
            localInputSchemas[0].addVariable(usedVar);
        for (i = 0; i < n; i++) {
            AggregateFunctionCallExpression mergeFun = (AggregateFunctionCallExpression) aggOp.getMergeExpressions()
                    .get(i).getValue();
            merges[i] = expressionRuntimeProvider.createSerializableAggregateFunctionFactory(mergeFun, aggOpInputEnv,
                    localInputSchemas, context);
        }
        IAggregatorDescriptorFactory aggregatorFactory = new SerializableAggregatorDescriptorFactory(aff);
        IAggregatorDescriptorFactory mergeFactory = new SerializableAggregatorDescriptorFactory(merges);

        INormalizedKeyComputerFactory normalizedKeyFactory = JobGenHelper.variablesToAscNormalizedKeyComputerFactory(
                gbyCols, aggOpInputEnv, context);

        IOperatorDescriptor gbyOpDesc = null;
        try {
            gbyOpDesc = getHyracksGroupByOperator(spec, keys, intermediateKeys, comparatorFactories,
                    hashFunctionFactories, hashFunctionFamilies, normalizedKeyFactory, aggregatorFactory, mergeFactory,
                    recordDescriptor);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }

        if (gbyOpDesc == null) {
            throw new AlgebricksException(gby.getOperatorTag() + ": failed to generate physical group-by operator!");
        }

        contributeOpDesc(builder, gby, gbyOpDesc);
        ILogicalOperator src = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, op, 0);
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator#isMicroOperator()
     */
    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + columnSet;
    }

    public List<LogicalVariable> getGbyColumns() {
        return columnSet;
    }

    public abstract IOperatorDescriptor getHyracksGroupByOperator(IOperatorDescriptorRegistry spec, int[] inputKeys,
            int[] intermediateKeys, IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFactory[] hashFunctionFactories, IBinaryHashFunctionFamily[] hashFunctionFamilies,
            INormalizedKeyComputerFactory nkf, IAggregatorDescriptorFactory aggFactory,
            IAggregatorDescriptorFactory mergeFactory, RecordDescriptor outRecDesc) throws HyracksDataException;

}
