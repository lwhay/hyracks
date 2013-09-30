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
package edu.uci.ics.hyracks.dataflow.std.group.global.costmodels;

public class CostVector {
    double cpu, io, network;

    public double getCpu() {
        return cpu;
    }

    public void setCpu(double cpu) {
        this.cpu = cpu;
    }

    public void updateCpu(double cpuDelta) {
        this.cpu += cpuDelta;
    }

    public double getIo() {
        return io;
    }

    public void setIo(double io) {
        this.io = io;
    }

    public void updateIo(double ioDelta) {
        this.io += ioDelta;
    }

    public double getNetwork() {
        return network;
    }

    public void setNetwork(double network) {
        this.network = network;
    }

    public void updateNetwork(double networkDelta) {
        this.network += networkDelta;
    }

    public void updateWithCostVector(CostVector cv) {
        this.cpu += cv.cpu;
        this.io += cv.io;
        this.network += cv.network;
    }

    public CostVector() {
        this.cpu = 0;
        this.io = 0;
        this.network = 0;
    }
}
