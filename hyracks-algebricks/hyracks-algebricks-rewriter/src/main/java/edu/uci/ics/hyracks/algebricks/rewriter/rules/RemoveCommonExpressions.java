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
package edu.uci.ics.hyracks.algebricks.rewriter.rules;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class RemoveCommonExpressions implements IAlgebraicRewriteRule {

    private final CommonExpressionSubstitutionVisitor substVisitor = new CommonExpressionSubstitutionVisitor();
    private final Map<ILogicalExpression, ExprEquivalenceClass> exprEqClassMap = new HashMap<ILogicalExpression, ExprEquivalenceClass>();
    private boolean recomputeTypes = false; 
    
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        if (recomputeTypes) {
            ILogicalOperator op = opRef.getValue();
            context.computeAndSetTypeEnvironmentForOperator(op);
            return false;
        }
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        exprEqClassMap.clear();
        substVisitor.setContext(context);
        boolean modified = removeCommonExpressions(opRef, context);
        if (modified) {
            context.computeAndSetTypeEnvironmentForOperator(opRef.getValue());
            recomputeTypes = true;
        }
        return modified;
    }

    private void updateEquivalenceClassMap(LogicalVariable lhs, Mutable<ILogicalExpression> rhsExprRef, ILogicalOperator op) {
        ExprEquivalenceClass exprEqClass = exprEqClassMap.get(rhsExprRef.getValue());
        if (exprEqClass == null) {
            exprEqClass = new ExprEquivalenceClass(op, rhsExprRef);
            exprEqClassMap.put(rhsExprRef.getValue(), exprEqClass);
        }
        exprEqClass.setVariable(lhs);
    }

    private boolean removeCommonExpressions(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        boolean modified = false;

        // Recurse into children.
        for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
            if (removeCommonExpressions(inputOpRef, context)) {
                modified = true;
            }
        }
        
        // Exclude these operators.
        if (op.getOperatorTag() == LogicalOperatorTag.UNNEST || op.getOperatorTag() == LogicalOperatorTag.AGGREGATE || op.getOperatorTag() == LogicalOperatorTag.RUNNINGAGGREGATE || op.getOperatorTag() == LogicalOperatorTag.GROUP) {
            return modified;
        }
        substVisitor.setOperator(op);
        if (op.acceptExpressionTransform(substVisitor)) {
            modified = true;
        }
        
        // Update equivalence class map.
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator assignOp = (AssignOperator) op;
            int numVars = assignOp.getVariables().size();
            for (int i = 0; i < numVars; i++) {
                Mutable<ILogicalExpression> exprRef = assignOp.getExpressions().get(i);
                ILogicalExpression expr = exprRef.getValue();
                if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE
                        || expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                    continue;
                }
                // Update equivalence class map.
                LogicalVariable lhs = assignOp.getVariables().get(i);
                updateEquivalenceClassMap(lhs, exprRef, op);
            }
        }

        // Perform replacement in nested plans.
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans opWithNestedPlan = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan nestedPlan : opWithNestedPlan.getNestedPlans()) {
                for (Mutable<ILogicalOperator> rootRef : nestedPlan.getRoots()) {
                    if (removeCommonExpressions(rootRef, context)) {
                        modified = true;
                    }
                }
            }
        }

        if (modified) {
            context.computeAndSetTypeEnvironmentForOperator(op);
            context.addToDontApplySet(this, op);
        }
        return modified;
    }

    private class CommonExpressionSubstitutionVisitor implements ILogicalExpressionReferenceTransform {
                
        private final Set<LogicalVariable> liveVars = new HashSet<LogicalVariable>();
        private IOptimizationContext context;
        private ILogicalOperator op;        
        
        public void setContext(IOptimizationContext context) {
            this.context = context;
        }
        
        public void setOperator(ILogicalOperator op) throws AlgebricksException {
            this.op = op;
            liveVars.clear();
        }
        
        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            if (liveVars.isEmpty()) {
                VariableUtilities.getLiveVariables(op, liveVars);
            }
            
            AbstractLogicalExpression expr = (AbstractLogicalExpression) exprRef.getValue();
            boolean modified = false;
            ExprEquivalenceClass exprEqClass = exprEqClassMap.get(expr);
            if (exprEqClass != null) {
                // Replace common subexpression with existing variable. 
                if (exprEqClass.variableIsSet()) {
                    if (liveVars.contains(exprEqClass.getVariable())) {
                        exprRef.setValue(new VariableReferenceExpression(exprEqClass.getVariable()));                        
                    }
                    // Do not descend into children since this expr has been completely replaced.
                    return true;
                } else {
                    if (assignCommonExpression(exprEqClass)) {
                        exprRef.setValue(new VariableReferenceExpression(exprEqClass.getVariable()));
                        return true;
                    }
                }
            } else {
                if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE
                        && expr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                    exprEqClass = new ExprEquivalenceClass(op, exprRef);
                    exprEqClassMap.put(expr, exprEqClass);
                }
            }
            
            // Descend into function arguments.
            if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
                for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
                    if (transform(arg)) {
                        modified = true;
                    }
                }
            }
            return modified;
        }
        
        private boolean assignCommonExpression(ExprEquivalenceClass exprEqClass) throws AlgebricksException {
            // TODO: Deal with joins and other binary ops.
            AbstractLogicalOperator firstOp = (AbstractLogicalOperator) exprEqClass.getFirstOperator();
            if (firstOp.getInputs().size() > 1) {
                return false;
            }
            
            Mutable<ILogicalExpression> firstExprRef = exprEqClass.getFirstExpression();
            LogicalVariable newVar = context.newVar();
            AssignOperator newAssign = new AssignOperator(newVar, new MutableObject<ILogicalExpression>(firstExprRef.getValue().cloneExpression()));            
            // Place assign below firstOp.
            newAssign.getInputs().add(new MutableObject<ILogicalOperator>(firstOp.getInputs().get(0).getValue()));
            firstOp.getInputs().get(0).setValue(newAssign);
            // Replace original expr with variable reference, and set var in expression equivalence class.
            firstExprRef.setValue(new VariableReferenceExpression(newVar));
            exprEqClass.setVariable(newVar);
            context.computeAndSetTypeEnvironmentForOperator(newAssign);
            context.computeAndSetTypeEnvironmentForOperator(firstOp);
            return true;
        }
    }
    
    private final class ExprEquivalenceClass {
        // First operator in which expression is used.
        private final ILogicalOperator firstOp;
        // Reference to expression in first op.
        private final Mutable<ILogicalExpression> firstExprRef;
        
        // Variable that this expression has been assigned to.
        private LogicalVariable var;
        
        public ExprEquivalenceClass(ILogicalOperator firstOp, Mutable<ILogicalExpression> firstExprRef) {
            this.firstOp = firstOp;
            this.firstExprRef = firstExprRef;
        }
        
        public ILogicalOperator getFirstOperator() {
            return firstOp;
        }
        
        public Mutable<ILogicalExpression> getFirstExpression() {
            return firstExprRef;
        }
        
        public void setVariable(LogicalVariable var) {
            this.var = var;
        }
        
        public LogicalVariable getVariable() {
            return var;
        }
        
        public boolean variableIsSet() {
            return var != null;
        }
    }
}
