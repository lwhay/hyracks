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
package edu.uci.ics.hyracks.control.nc.comm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionManager;
import edu.uci.ics.hyracks.api.comm.PartitionId;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileHandle;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IFileFactory;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.io.IIORequest;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.control.nc.io.IORequest;

public class PartitionManager implements IPartitionManager {
    private final NodeControllerService ncs;

    public PartitionManager(NodeControllerService ncs) {
        this.ncs = ncs;
    }

    private void registerPartition(PartitionId partitionId, PartitionInfo pInfo) throws HyracksDataException {
        Map<PartitionId, PartitionInfo> partitionMap = ncs.getPartitionMap(partitionId.getJobId());
        if (partitionMap == null) {
            throw new HyracksDataException("No partition map for the given partition: " + partitionId);
        }
        partitionMap.put(partitionId, pInfo);
    }

    @Override
    public IFrameWriter createPartitionWriter(IHyracksContext ctx, PartitionId partitionId) throws HyracksDataException {
        return new PartitionWriter(ctx, partitionId);
    }

    private class PartitionWriter implements IFrameWriter, IIORequest.INotificationCallback {
        private IIOManager ioManager;
        private PartitionId partitionId;
        private PartitionInfo pInfo;
        private FileHandle fHandle;
        private long offset;

        public PartitionWriter(IHyracksContext ctx, PartitionId partitionId) {
            this.ioManager = ctx.getIOManager();
            this.partitionId = partitionId;
        }

        @Override
        public void open() throws HyracksDataException {
            IFileFactory fFactory = ioManager.getDeviceManager().getTempFileFactory();
            FileReference fRef = fFactory.createFile(partitionId.toString(), ".run");
            fHandle = new FileHandle(fRef);
            try {
                fHandle.open();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
            pInfo = new PartitionInfo(partitionId, fRef);
            registerPartition(partitionId, pInfo);
            offset = 0;
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            int len = buffer.remaining();
            ioManager.write(fHandle, offset, this, buffer);
            offset += len;
        }

        @Override
        public void flush() throws HyracksDataException {
        }

        @Override
        public void close() throws HyracksDataException {
            ioManager.close(fHandle, this);
        }

        @Override
        public void success(IIORequest request) {
            if (request.getType() == IORequest.RequestType.WRITE) {
                pInfo.incrementFrameCount();
            } else if (request.getType() == IORequest.RequestType.CLOSE) {
                pInfo.setDone();
            }
        }

        @Override
        public void failure(IIORequest request, IOException e) {

        }
    }
}