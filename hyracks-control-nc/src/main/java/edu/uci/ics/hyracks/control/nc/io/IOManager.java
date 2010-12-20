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
package edu.uci.ics.hyracks.control.nc.io;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import edu.uci.ics.hyracks.api.io.FileHandle;
import edu.uci.ics.hyracks.api.io.IDeviceManager;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.io.IIORequest;

public class IOManager implements IIOManager {
    private final DeviceManager deviceManager;
    private final Queue<IORequest> freeRequestQueue;
    private IOThread[] ioThreads;

    public IOManager(int frameSize, DeviceManager deviceManager, int nBuffers) {
        this.deviceManager = deviceManager;
        freeRequestQueue = new ArrayBlockingQueue<IORequest>(nBuffers);
        for (int i = 0; i < nBuffers; ++i) {
            IORequest req = new IORequest(ByteBuffer.allocateDirect(frameSize));
            freeRequestQueue.add(req);
        }
        ioThreads = new IOThread[deviceManager.getDevices().length];
        for (int i = 0; i < ioThreads.length; ++i) {
            ioThreads[i] = new IOThread(this);
        }
    }

    @Override
    public IDeviceManager getDeviceManager() {
        return deviceManager;
    }

    @Override
    public void read(FileHandle fHandle, long offset, IIORequest.INotificationCallback callback, ByteBuffer data) {
        IORequest request = freeRequestQueue.poll();
        request.setRequest(IORequest.RequestType.READ, fHandle, offset, callback);
        ByteBuffer sysBuffer = request.getBuffer();
        sysBuffer.clear();
        sysBuffer.put(data);
        sysBuffer.flip();
        ioThreads[fHandle.getFileReference().getDevice().getDeviceId()].schedule(request);
    }

    @Override
    public void write(FileHandle fHandle, long offset, IIORequest.INotificationCallback callback, ByteBuffer data) {
        IORequest request = freeRequestQueue.poll();
        request.setRequest(IORequest.RequestType.WRITE, fHandle, offset, callback);
        ByteBuffer sysBuffer = request.getBuffer();
        sysBuffer.clear();
        sysBuffer.put(data);
        sysBuffer.flip();
        ioThreads[fHandle.getFileReference().getDevice().getDeviceId()].schedule(request);
    }

    @Override
    public void close(FileHandle fHandle, IIORequest.INotificationCallback callback) {
        IORequest request = freeRequestQueue.poll();
        request.setRequest(IORequest.RequestType.CLOSE, fHandle, 0, callback);
        ioThreads[fHandle.getFileReference().getDevice().getDeviceId()].schedule(request);
    }

    public void free(IORequest request) {
        request.setRequest(null, null, 0, null);
        freeRequestQueue.add(request);
    }
}