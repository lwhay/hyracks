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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;

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
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class RemoveCommonExpressions implements IAlgebraicRewriteRule {

    private final CommonExpressionSubstitutionVisitor substVisitor = new CommonExpressionSubstitutionVisitor();
    private final Map<ILogicalExpression, ExprEquivalenceClass> exprEqClassMap = new HashMap<ILogicalExpression, ExprEquivalenceClass>();
    
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        exprEqClassMap.clear();
        boolean modified = removeCommonExpressions(opRef, context);
        if (modified) {
            context.computeAndSetTypeEnvironmentForOperator(opRef.getValue());
        }
        return modified;
    }

    private void updateEquivalenceClassMap(LogicalVariable lhs, ILogicalExpression rhs, ILogicalOperator op) {
        ExprEquivalenceClass exprEqClass = exprEqClassMap.get(rhs);
        if (exprEqClass == null) {
            exprEqClass = new ExprEquivalenceClass();
            exprEqClassMap.put(rhs, exprEqClass);
        }
        exprEqClass.addVariable(lhs);
        exprEqClass.addOperator(op);
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

        substVisitor.setOperator(op);
        if (op.acceptExpressionTransform(substVisitor)) {
            modified = true;
        }
        
        // Update equivalence class map.
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator assignOp = (AssignOperator) op;
            int numVars = assignOp.getVariables().size();
            for (int i = 0; i < numVars; i++) {
                ILogicalExpression expr = assignOp.getExpressions().get(i).getValue();
                if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE
                        || expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                    continue;
                }
                // Update equivalence class map.
                LogicalVariable lhs = assignOp.getVariables().get(i);
                updateEquivalenceClassMap(lhs, expr, op);
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
        private ILogicalOperator op;
        
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
                if (exprEqClass.hasVariables()) {
                    // Find an existing live variable to replace the common expr.
                    for (LogicalVariable var : exprEqClass.getVariables()) {
                        if (liveVars.contains(var)) {
                            exprRef.setValue(new VariableReferenceExpression(var));
                            modified = true;
                        }
                    }
                }
            } else {
                exprEqClass = new ExprEquivalenceClass();                
                exprEqClassMap.put(expr, exprEqClass);
            }
            exprEqClass.addOperator(op);
            
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
    }
    
    private final class ExprEquivalenceClass {
        // List of operators in which expression is used.
        private final List<ILogicalOperator> ops = new ArrayList<ILogicalOperator>();
        
        // List of variables that this expression has been assigned to.
        private final List<LogicalVariable> vars = new ArrayList<LogicalVariable>();
        
        public void addVariable(LogicalVariable var) {
            if (!vars.contains(var)) {
                vars.add(var);
            }
        }
        
        public void addOperator(ILogicalOperator op) {
            if (ops.isEmpty() || ops.get(ops.size() - 1) != op) {
                ops.add(op);
            }
        }
        
        public List<ILogicalOperator> getOperators() {
            return ops;
        }
        
        public List<LogicalVariable> getVariables() {
            return vars;
        }
        
        public boolean hasVariables() {
            return !vars.isEmpty();
        }
    }
}
