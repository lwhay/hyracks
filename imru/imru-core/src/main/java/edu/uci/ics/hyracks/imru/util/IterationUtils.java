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
package edu.uci.ics.hyracks.imru.util;

import java.util.Map;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.state.IStateObject;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRURuntimeContext;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.StateKey;

public class IterationUtils {

    public static void setIterationState(IHyracksTaskContext ctx, int partition, IStateObject state) {
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        IMRURuntimeContext context = (IMRURuntimeContext) appContext.getApplicationObject();
        Map<StateKey, IStateObject> map = context.getAppStateStore();
        map.put(new StateKey(ctx.getJobletContext().getJobId(), partition), state);
    }

    public static IStateObject getIterationState(IHyracksTaskContext ctx, int partition) {
        JobId currentId = ctx.getJobletContext().getJobId();
        JobId lastId = new JobId(currentId.getId() - 2);
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        IMRURuntimeContext context = (IMRURuntimeContext) appContext.getApplicationObject();
        Map<StateKey, IStateObject> map = context.getAppStateStore();
        return map.get(new StateKey(lastId, partition));
    }

    public static void removeIterationState(IHyracksTaskContext ctx, int partition) {
        JobId currentId = ctx.getJobletContext().getJobId();
        JobId lastId = new JobId(currentId.getId() - 1);
        INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
        IMRURuntimeContext context = (IMRURuntimeContext) appContext.getApplicationObject();
        Map<StateKey, IStateObject> map = context.getAppStateStore();
        map.remove(new StateKey(lastId, partition));
    }

}
