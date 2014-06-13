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

package edu.uci.ics.hyracks.control.nc.work;

import java.net.URL;
import java.util.List;

import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.control.common.base.IClusterController;
import edu.uci.ics.hyracks.control.common.deployment.DeploymentStatus;
import edu.uci.ics.hyracks.control.common.deployment.DeploymentUtils;
import edu.uci.ics.hyracks.control.common.work.AbstractWork;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;

public class ShutdownWork extends AbstractWork {
    
    private final NodeControllerService ncs;
    public ShutdownWork(NodeControllerService ncs) {
        this.ncs = ncs;
    }

    @Override
    public void run() {
        try {
            IClusterController ccs = ncs.getClusterController();
            ccs.notifyShutdown(ncs.getId());
            ncs.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
