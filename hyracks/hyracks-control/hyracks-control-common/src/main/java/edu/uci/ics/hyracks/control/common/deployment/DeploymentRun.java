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

package edu.uci.ics.hyracks.control.common.deployment;

import java.util.Set;
import java.util.TreeSet;

public class DeploymentRun implements IDeploymentStatusConditionVariable {

    private DeploymentStatus deploymentStatus = DeploymentStatus.FAIL;
    private final Set<String> deploymentNodeIds = new TreeSet<String>();

    public DeploymentRun(Set<String> nodeIds) {
        deploymentNodeIds.addAll(nodeIds);
    }

    public synchronized void notifyDeploymentStatus(String nodeId, DeploymentStatus status) {
        deploymentNodeIds.remove(nodeId);
        if (deploymentNodeIds.size() == 0) {
            deploymentStatus = DeploymentStatus.SUCCEED;
            notifyAll();
        }
    }

    @Override
    public synchronized DeploymentStatus waitForCompletion() throws Exception {
        wait();
        return deploymentStatus;
    }

}
