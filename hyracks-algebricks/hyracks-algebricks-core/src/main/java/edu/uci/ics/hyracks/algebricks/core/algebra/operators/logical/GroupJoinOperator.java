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
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.INestedPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.TypePropagationPolicy;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypeEnvPointer;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypingContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.OpRefTypeEnvPointer;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.PropagatingTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import edu.uci.ics.hyracks.algebricks.core.config.AlgebricksConfig;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;

public class GroupJoinOperator extends AbstractBinaryJoinOperator implements INestedPlan {

    private final List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> gByList;
    private final List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> decorList;
    private Mutable<List<ILogicalPlan>> nestedPlans;
    
    public GroupJoinOperator(JoinKind joinKind, Mutable<ILogicalExpression> condition,
            Mutable<ILogicalOperator> input1, Mutable<ILogicalOperator> input2) {
        super(JoinKind.LEFT_OUTER, condition);
        gByList = new ArrayList<Pair<LogicalVariable, Mutable<ILogicalExpression>>>();
        decorList = new ArrayList<Pair<LogicalVariable, Mutable<ILogicalExpression>>>();
        this.nestedPlans = new MutableObject<List<ILogicalPlan>>();
    }

    public GroupJoinOperator(Mutable<ILogicalExpression> condition) {
        super(JoinKind.LEFT_OUTER, condition);
        gByList = new ArrayList<Pair<LogicalVariable, Mutable<ILogicalExpression>>>();
        decorList = new ArrayList<Pair<LogicalVariable, Mutable<ILogicalExpression>>>();
        this.nestedPlans = new MutableObject<List<ILogicalPlan>>();
    }

    public GroupJoinOperator(JoinKind joinKind, Mutable<ILogicalExpression> condition,
            Mutable<ILogicalOperator> input1, Mutable<ILogicalOperator> input2,
            List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> groupByList,
            List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> decorList, List<ILogicalPlan> nestedPlans) throws AlgebricksException {
        super(joinKind, condition, input1, input2);
        this.gByList = groupByList;
        this.decorList = decorList;
        this.nestedPlans = new MutableObject<List<ILogicalPlan>>(nestedPlans);
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.GROUPJOIN;
    }

    public boolean hasNestedPlans(){
    	return true;
    }
    
    public List<ILogicalPlan> getNestedPlans() {
        return nestedPlans.getValue();
    }
    
    public void setNestedPlans(List<ILogicalPlan> nPlans) {
        this.nestedPlans.setValue(nPlans);
    }
    
    public List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> getGroupByList() {
        return gByList;
    }

    public List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> getDecorList() {
        return decorList;
    }

    public String gByListToString() {
        return veListToString(gByList);
    }

    public String decorListToString() {
        return veListToString(decorList);
    }

    public List<LogicalVariable> getGbyVarList() {
        List<LogicalVariable> varList = new ArrayList<LogicalVariable>(gByList.size());
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> ve : gByList) {
            ILogicalExpression expr = ve.second.getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression v = (VariableReferenceExpression) expr;
                varList.add(v.getVariableReference());
            }
        }
        return varList;
    }

    public static String veListToString(List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> vePairList) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean fst = true;
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> ve : vePairList) {
            if (fst) {
                fst = false;
            } else {
                sb.append("; ");
            }
            if (ve.first != null) {
                sb.append(ve.first + " := " + ve.second);
            } else {
                sb.append(ve.second.getValue());
            }
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitGroupJoinOperator(this, arg);
    }

    @Override
    public void recomputeSchema(){
        schema = new ArrayList<LogicalVariable>();
//        schema.addAll(inputs.get(0).getValue().getSchema());
//        schema.addAll(inputs.get(1).getValue().getSchema());
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gByList) {
            if (p.first != null)
            	schema.add(p.first);
        }
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
            schema.add(getDecorVariable(p));
        }
        
        for(ILogicalPlan p : getNestedPlans()){
        	if (p == null)
        		continue;
        	for(Mutable<ILogicalOperator> o : p.getRoots()){
        		AbstractLogicalOperator nestedOp = (AbstractLogicalOperator)o.getValue();
        		if (nestedOp != null){
        			schema.addAll(nestedOp.getSchema());
        		}
        	}
        }
    }

	@Override
	public LinkedList<Mutable<ILogicalOperator>> allRootsInReverseOrder() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void getProducedVariablesExceptNestedPlans(Collection<LogicalVariable> vars) {
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gByList) {
            if (p.first != null) {
                vars.add(p.first);
            }
        }
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
            if (p.first != null) {
                vars.add(p.first);
            }
        }
	}

	@Override
	public void getUsedVariablesExceptNestedPlans(Collection<LogicalVariable> vars) {
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> g : gByList) {
            g.second.getValue().getUsedVariables(vars);
        }
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> g : decorList) {
            g.second.getValue().getUsedVariables(vars);
        }
        getCondition().getValue().getUsedVariables(vars);
	}
	
    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        ILogicalOperator child1 = inputs.get(0).getValue();
        ILogicalOperator child2 = inputs.get(1).getValue();
        IVariableTypeEnvironment env1 = ctx.getOutputTypeEnvironment(child1);
        IVariableTypeEnvironment env2 = ctx.getOutputTypeEnvironment(child2);
        int n = 0;
        for (ILogicalPlan p : getNestedPlans()) {
            n += p.getRoots().size();
        }
        ITypeEnvPointer[] envPointers = new ITypeEnvPointer[n];
        int i = 0;
        for (ILogicalPlan p : getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
//                ctx.setOutputTypeEnvironment(r.getValue(), env1);
                envPointers[i] = new OpRefTypeEnvPointer(r, ctx);
                i++;
            }
        }
        
        IVariableTypeEnvironment env;
        
        if(getJoinKind() == JoinKind.LEFT_OUTER){
        	env = new PropagatingTypeEnvironment(ctx.getExpressionTypeComputer(),
                ctx.getNullableTypeComputer(), ctx.getMetadataProvider(), TypePropagationPolicy.LEFT_OUTER, envPointers);
        }
        else
        	env = new PropagatingTypeEnvironment(ctx.getExpressionTypeComputer(),
                    ctx.getNullableTypeComputer(), ctx.getMetadataProvider(), TypePropagationPolicy.ALL, envPointers);
        
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : getGroupByList()) {
            ILogicalExpression expr = p.second.getValue();
            if (p.first != null) {
                env.setVarType(p.first, env1.getType(expr));
                if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                    LogicalVariable v1 = ((VariableReferenceExpression) expr).getVariableReference();
                    env.setVarType(v1, env1.getVarType(v1));
                }
            } else {
                VariableReferenceExpression vre = (VariableReferenceExpression) p.second.getValue();
                LogicalVariable v2 = vre.getVariableReference();
                env.setVarType(v2, env1.getVarType(v2));
            }
        }
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : getDecorList()) {
            ILogicalExpression expr = p.second.getValue();
            if (p.first != null) {
                env.setVarType(p.first, env1.getType(expr));
            } else {
                VariableReferenceExpression vre = (VariableReferenceExpression) p.second.getValue();
                LogicalVariable v2 = vre.getVariableReference();
                env.setVarType(v2, env1.getVarType(v2));
            }
        }
        return env;
    }

	@Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
		VariablePropagationPolicy groupJoinVarPropPolicy = new VariablePropagationPolicy() {

            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gByList) {
                    ILogicalExpression expr = p.second.getValue();
                    if (p.first != null) {
                        target.addVariable(p.first);
                    } else {
                        if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                            throw new AlgebricksException("GroupJoin expects variable references.");
                        }
                        VariableReferenceExpression v = (VariableReferenceExpression) expr;
                        target.addVariable(v.getVariableReference());
                    }
                }
                for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
                    ILogicalExpression expr = p.second.getValue();
                    if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                        throw new AlgebricksException("GroupJoin expects variable references.");
                    }
                    VariableReferenceExpression v = (VariableReferenceExpression) expr;
                    LogicalVariable decor = v.getVariableReference();
                    if (p.first != null) {
                        target.addVariable(p.first);
                    } else {
                        target.addVariable(decor);
                    }
                }
            }
        };
        
        for(ILogicalPlan p : getNestedPlans()){
        	if (p == null)
        		continue;
        	for(Mutable<ILogicalOperator> o : p.getRoots()){
        		AbstractLogicalOperator nestedOp = (AbstractLogicalOperator)o.getValue();
        		if (nestedOp.getOperatorTag() == LogicalOperatorTag.AGGREGATE)
        			groupJoinVarPropPolicy = VariablePropagationPolicy.concat(groupJoinVarPropPolicy, nestedOp.getVariablePropagationPolicy());
        	}
        }
        
        return groupJoinVarPropPolicy;
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        boolean b = false;
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gByList) {
            if (visitor.transform(p.second)) {
                b = true;
            }
        }
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
            if (visitor.transform(p.second)) {
                b = true;
            }
        }
        return b;
    }

    public static LogicalVariable getDecorVariable(Pair<LogicalVariable, Mutable<ILogicalExpression>> p) {
        if (p.first != null) {
            return p.first;
        } else {
            VariableReferenceExpression e = (VariableReferenceExpression) p.second.getValue();
            return e.getVariableReference();
        }
    }


}
