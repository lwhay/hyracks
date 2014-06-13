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

package edu.uci.ics.hyracks.control.cc.work;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.common.deployment.DeploymentRun;
import edu.uci.ics.hyracks.control.common.deployment.DeploymentUtils;
import edu.uci.ics.hyracks.control.common.shutdown.ShutdownRun;
import edu.uci.ics.hyracks.control.common.work.IPCResponder;
import edu.uci.ics.hyracks.control.common.work.SynchronizableWork;

public class ClusterShutdownWork extends SynchronizableWork {

    private ClusterControllerService ccs;
    private IPCResponder<Boolean> callback;

    public ClusterShutdownWork(ClusterControllerService ncs, IPCResponder<Boolean> callback) {
        this.ccs = ncs;
        this.callback = callback;
    }

    @Override
    public void doRun() {
        try {
            Map<String, NodeControllerState> nodeControllerStateMap = ccs.getNodeMap();
            Set<String> nodeIds = new TreeSet<String>();
            for (String nc : nodeControllerStateMap.keySet()) {
                nodeIds.add(nc);
            }
            /**
             * set up our listener for the node ACKs
             */
            final ShutdownRun shutdownStatus = new ShutdownRun(nodeIds);
            /***
             * Shutdown all the nodes...
             */
            for (NodeControllerState ncs : nodeControllerStateMap.values()) {
                ncs.getNodeController().shutDown();
            }
            ccs.getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        /**
                         * wait for all our acks
                         */
                        boolean cleanShutdown = shutdownStatus.waitForCompletion();
                        if(cleanShutdown){
                            ccs.stop();
                            callback.setValue(new Boolean(true));
                        }
                        /**
                         * see if the heartbeats stopped at least 
                         */
                        else{
                        }
                        //TODO: what to really call here?
                        
                    } catch (Exception e) {
                        callback.setException(e);
                    }
                }
            });
        } catch (Exception e) {
            callback.setException(e);
        }
    }
}
