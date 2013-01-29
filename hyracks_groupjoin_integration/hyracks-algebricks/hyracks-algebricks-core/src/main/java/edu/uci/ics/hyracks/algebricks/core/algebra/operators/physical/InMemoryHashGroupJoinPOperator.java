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
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.aggreg.SerializableAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.join.InMemoryHashGroupJoinOperatorDescriptor;

public class InMemoryHashGroupJoinPOperator extends AbstractHashJoinPOperator {

    private final int tableSize;

    /**
     * builds on the first operator and probes on the second.
     */

    public InMemoryHashGroupJoinPOperator(JoinKind kind, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeftOfEqualities, List<LogicalVariable> sideRightOfEqualities, int tableSize) {
        super(kind, partitioningType, sideLeftOfEqualities, sideRightOfEqualities);
        this.tableSize = tableSize;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.IN_MEMORY_HASH_GROUP_JOIN;
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + " " + keysLeftBranch + keysRightBranch;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        GroupJoinOperator opLogical = (GroupJoinOperator) op;
        int[] keysLeft = JobGenHelper.variablesToFieldIndexes(keysLeftBranch, inputSchemas[0]);
        int[] keysRight = JobGenHelper.variablesToFieldIndexes(keysRightBranch, inputSchemas[1]);
        IVariableTypeEnvironment env = context.getTypeEnvironment(op);
        
        INullWriterFactory[] nullWriterFactories;
        switch (kind) {
        case INNER: {
        	nullWriterFactories = null;
            break;
        	}
        case LEFT_OUTER: {
            Collection<LogicalVariable> nestedProdVars = new HashSet<LogicalVariable>();
            for(ILogicalPlan p : opLogical.getNestedPlans()){
            	VariableUtilities.getProducedVariables(p.getRoots().get(0).getValue(), nestedProdVars);
            }
            nullWriterFactories = new INullWriterFactory[nestedProdVars.size()];
            for (int j = 0; j < nullWriterFactories.length; j++) {
                nullWriterFactories[j] = context.getNullWriterFactory();
            }
            break;
        	}
        default: {
            throw new NotImplementedException();
        	}
        }

        IBinaryHashFunctionFactory[] hashFunFactories = JobGenHelper.variablesToBinaryHashFunctionFactories(
                keysLeftBranch, env, context);
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[keysLeft.length];
        int i = 0;
        IBinaryComparatorFactoryProvider bcfp = context.getBinaryComparatorFactoryProvider();
        for (LogicalVariable v : keysLeftBranch) {
            Object t = env.getVarType(v);
            comparatorFactories[i++] = bcfp.getBinaryComparatorFactory(t, true);
        }
        RecordDescriptor recDescriptor = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema, context);
        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        IOperatorDescriptor opDesc = null;
        
        int numDecors = opLogical.getDecorList().size();
        int[] decorKeys = new int[numDecors];
        int j = 0;
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : opLogical.getDecorList()) {
            ILogicalExpression expr = p.second.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                throw new AlgebricksException("groupjoin expects variable references.");
            }
            VariableReferenceExpression v = (VariableReferenceExpression) expr;
            LogicalVariable decor = v.getVariableReference();
            decorKeys[j++] = inputSchemas[0].findVariable(decor);
        }

/*        if (opLogical.getNestedPlans().size() != 1) {
            throw new AlgebricksException(
                    "External group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
        ILogicalPlan p0 = opLogical.getNestedPlans().get(0);
        if (p0.getRoots().size() != 1) {
            throw new AlgebricksException(
                    "External group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
*/
        
        int n = 0;
        AggregateOperator aggOp;
        List<AggregateOperator> aggOpList = new LinkedList<AggregateOperator>();
        AbstractLogicalOperator nestedOp;

        for(Mutable<ILogicalOperator> nOp : opLogical.getNestedPlans().get(0).getRoots()) {
        	nestedOp = (AbstractLogicalOperator) nOp.getValue();
        	if(nestedOp.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                aggOp = (AggregateOperator) nestedOp;
                n += aggOp.getExpressions().size();
                aggOpList.add(aggOp);
        	}
        }
        
        ICopySerializableAggregateFunctionFactory[] aff = new ICopySerializableAggregateFunctionFactory[n];
        IExpressionRuntimeProvider expressionRuntimeProvider = context.getExpressionRuntimeProvider();
//        ILogicalExpressionJobGen exprJobGen = context.getExpressionJobGen();
        IVariableTypeEnvironment aggOpInputEnv;
//        IVariableTypeEnvironment outputEnv = context.getTypeEnvironment(op);
        compileSubplans(inputSchemas[1], opLogical, propagatedSchema, context);
        i=0;
        for(AggregateOperator a : aggOpList) {
        	aggOpInputEnv = context.getTypeEnvironment(a.getInputs().get(0).getValue());
            for (Mutable<ILogicalExpression> exprRef : a.getExpressions()) {
                AggregateFunctionCallExpression aggFun = (AggregateFunctionCallExpression) exprRef.getValue();
                aff[i++] = expressionRuntimeProvider.createSerializableAggregateFunctionFactory(aggFun, aggOpInputEnv, inputSchemas,
                        context);
//                intermediateTypes.add(partialAggregationTypeComputer.getType(aggFun, aggOpInputEnv,
//                        context.getMetadataProvider()));
            }
        }
        IAggregatorDescriptorFactory aggregatorFactory = new SerializableAggregatorDescriptorFactory(aff);
        opDesc = new InMemoryHashGroupJoinOperatorDescriptor(spec, keysLeft, keysRight, decorKeys, hashFunFactories,
        		comparatorFactories, comparatorFactories, /* aggregatorFactory */ aggregatorFactory, recDescriptor,
        		opLogical.getJoinKind() == JoinKind.LEFT_OUTER, nullWriterFactories, tableSize);

        contributeOpDesc(builder, (AbstractLogicalOperator) op, opDesc);

        ILogicalOperator src1 = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src1, 0, op, 0);
        ILogicalOperator src2 = op.getInputs().get(1).getValue();
        builder.contributeGraphEdge(src2, 0, op, 1);
    }

    @Override
    protected List<ILocalStructuralProperty> deliveredLocalProperties(ILogicalOperator op, IOptimizationContext context) {
        AbstractLogicalOperator op0 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        IPhysicalPropertiesVector pv0 = op0.getPhysicalOperator().getDeliveredProperties();
        List<ILocalStructuralProperty> lp0 = pv0.getLocalProperties();
        if (lp0 != null) {
            // maintains the local properties on the probe side
            return new LinkedList<ILocalStructuralProperty>(lp0);
        }
        return new LinkedList<ILocalStructuralProperty>();
    }

}
