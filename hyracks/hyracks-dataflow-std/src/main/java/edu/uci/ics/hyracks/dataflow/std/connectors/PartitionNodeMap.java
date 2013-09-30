/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.connectors;

import java.util.BitSet;

public class PartitionNodeMap {

    int fromNodes, toNodes;

    BitSet nodeMap;

    public PartitionNodeMap(int fromNodes, int toNodes) {
        this.fromNodes = fromNodes;
        this.toNodes = toNodes;
        this.nodeMap = new BitSet(fromNodes * toNodes);
        this.nodeMap.clear(0, fromNodes * toNodes);
    }

    public int getFromNodes() {
        return fromNodes;
    }

    public int getToNodes() {
        return toNodes;
    }

    public void reset() {
        this.nodeMap.clear(0, fromNodes * toNodes);
    }

    public void setAsOneToOneNodeMap() {
        for (int i = 0; i < fromNodes; i++) {
            this.nodeMap.set(i * toNodes + i % toNodes);
        }
    }

    public void setAsFullyPartitionNodeMap() {
        this.nodeMap.set(0, fromNodes * toNodes);
    }

    public void setMap(int fromIndex, int toIndex) {
        this.nodeMap.set(fromIndex * toNodes + toIndex);
    }

    public boolean isFullyPartitionNodeMap() {
        for (int i = 0; i < fromNodes * toNodes; i++) {
            if (!nodeMap.get(i)) {
                return false;
            }
        }
        return true;
    }

    public boolean isOneToOnePartitionNodeMap() {
        if (fromNodes != toNodes) {
            return false;
        }
        if (nodeMap.cardinality() != toNodes) {
            return false;
        }
        for (int i = 0; i < fromNodes; i++) {
            if (!nodeMap.get(i * toNodes + i)) {
                return false;
            }
        }
        return true;
    }

    public BitSet getNodeMap() {
        return nodeMap;
    }
}
