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
package edu.uci.ics.hyracks.control.nc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;

import edu.uci.ics.hyracks.api.comm.PartitionId;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounterContext;
import edu.uci.ics.hyracks.api.resources.IDeallocatable;
import edu.uci.ics.hyracks.api.resources.IResourceDeallocator;
import edu.uci.ics.hyracks.api.resources.IResourceManager;
import edu.uci.ics.hyracks.control.nc.comm.PartitionInfo;

public class Joblet implements IResourceDeallocator, IDeallocatable, IHyracksJobletContext {
    private static final long serialVersionUID = 1L;

    private final NodeControllerService nodeController;

    private final IHyracksContext hyracksCtx;

    private final UUID jobId;

    private final Map<UUID, Stagelet> stageletMap;

    private final Map<OperatorDescriptorId, Map<Integer, IOperatorEnvironment>> envMap;

    private final Map<PartitionId, PartitionInfo> partitionMap;

    private final List<IDeallocatable> deallocatableResources;

    public Joblet(NodeControllerService nodeController, IHyracksContext hyracksCtx, UUID jobId) {
        this.nodeController = nodeController;
        this.hyracksCtx = hyracksCtx;
        this.jobId = jobId;
        stageletMap = new HashMap<UUID, Stagelet>();
        envMap = new HashMap<OperatorDescriptorId, Map<Integer, IOperatorEnvironment>>();
        partitionMap = new HashMap<PartitionId, PartitionInfo>();
        this.deallocatableResources = new ArrayList<IDeallocatable>();
    }

    @Override
    public UUID getJobId() {
        return jobId;
    }

    @Override
    public int getAttempt() {
        // TODO
        return 0;
    }

    public IOperatorEnvironment getEnvironment(IOperatorDescriptor hod, int partition) {
        if (!envMap.containsKey(hod.getOperatorId())) {
            envMap.put(hod.getOperatorId(), new HashMap<Integer, IOperatorEnvironment>());
        }
        Map<Integer, IOperatorEnvironment> opEnvMap = envMap.get(hod.getOperatorId());
        if (!opEnvMap.containsKey(partition)) {
            opEnvMap.put(partition, new OperatorEnvironmentImpl());
        }
        return opEnvMap.get(partition);
    }

    private static final class OperatorEnvironmentImpl implements IOperatorEnvironment {
        private final Map<String, Object> map;

        public OperatorEnvironmentImpl() {
            map = new HashMap<String, Object>();
        }

        @Override
        public Object get(String name) {
            return map.get(name);
        }

        @Override
        public void set(String name, Object value) {
            map.put(name, value);
        }
    }

    public Map<PartitionId, PartitionInfo> getPartitionMap() {
        return partitionMap;
    }

    public void setStagelet(UUID stageId, Stagelet stagelet) {
        stageletMap.put(stageId, stagelet);
    }

    public Stagelet getStagelet(UUID stageId) throws Exception {
        return stageletMap.get(stageId);
    }

    public Executor getExecutor() {
        return nodeController.getExecutor();
    }

    public synchronized void notifyStageletComplete(UUID stageId, int attempt, Map<String, Long> stats)
            throws Exception {
        stageletMap.remove(stageId);
        nodeController.notifyStageComplete(jobId, stageId, attempt, stats);
    }

    public void notifyStageletFailed(UUID stageId, int attempt) throws Exception {
        stageletMap.remove(stageId);
        nodeController.notifyStageFailed(jobId, stageId, attempt);
    }

    public NodeControllerService getNodeController() {
        return nodeController;
    }

    public void dumpProfile(Map<String, Long> counterDump) {
        Set<UUID> stageIds;
        synchronized (this) {
            stageIds = new HashSet<UUID>(stageletMap.keySet());
        }
        for (UUID stageId : stageIds) {
            Stagelet si;
            synchronized (this) {
                si = stageletMap.get(stageId);
            }
            if (si != null) {
                si.dumpProfile(counterDump);
            }
        }
    }

    @Override
    public void addDeallocatableResource(IDeallocatable resource) {
        deallocatableResources.add(resource);
    }

    @Override
    public void deallocate() {
        for (IDeallocatable resource : deallocatableResources) {
            try {
                resource.deallocate();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public IResourceManager getResourceManager() {
        return hyracksCtx.getResourceManager();
    }

    @Override
    public int getFrameSize() {
        return hyracksCtx.getFrameSize();
    }

    @Override
    public ICounterContext getCounterContext() {
        return hyracksCtx.getCounterContext();
    }

    @Override
    public IIOManager getIOManager() {
        return hyracksCtx.getIOManager();
    }
}