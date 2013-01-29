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
package edu.uci.ics.hyracks.algebricks.core.algebra.base;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;

public interface INestedPlan {
	
	public LogicalOperatorTag getOperatorTag();
	
    public List<ILogicalPlan> getNestedPlans();

    public boolean hasNestedPlans();
    
    public LinkedList<Mutable<ILogicalOperator>> allRootsInReverseOrder();

    //
    // @Override
    // public void computeConstraintsAndEquivClasses() {
    // for (ILogicalPlan p : nestedPlans) {
    // for (LogicalOperatorReference r : p.getRoots()) {
    // AbstractLogicalOperator op = (AbstractLogicalOperator) r.getOperator();
    // equivalenceClasses.putAll(op.getEquivalenceClasses());
    // functionalDependencies.addAll(op.getFDs());
    // }
    // }
    // }

    public void recomputeSchema();

    public boolean isMap();
    
    public void getUsedVariablesExceptNestedPlans(Collection<LogicalVariable> vars);

    public void getProducedVariablesExceptNestedPlans(Collection<LogicalVariable> vars);
}
