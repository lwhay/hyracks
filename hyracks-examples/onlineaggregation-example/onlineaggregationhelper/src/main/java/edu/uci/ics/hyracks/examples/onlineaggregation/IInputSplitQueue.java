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
package edu.uci.ics.hyracks.examples.onlineaggregation;

import java.rmi.Remote;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface IInputSplitQueue extends Remote {
    public MarshalledWritable<OnlineFileSplit> getNext(UUID jobId, int requestor) throws Exception;

    public Map<Integer, List<StatsRecord>> getStatistics(UUID jobId) throws Exception;
}