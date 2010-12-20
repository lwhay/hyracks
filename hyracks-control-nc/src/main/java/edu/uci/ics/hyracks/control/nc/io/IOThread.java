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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.hyracks.api.io.FileHandle;
import edu.uci.ics.hyracks.api.io.IIORequest;

public class IOThread extends Thread {
    private final IOManager ioMgr;
    private final Queue<IORequest> requestQueue;

    public IOThread(IOManager ioMgr) {
        this.ioMgr = ioMgr;
        requestQueue = new LinkedBlockingQueue<IORequest>();
        setDaemon(true);
    }

    public void schedule(IORequest request) {
        requestQueue.add(request);
    }

    @Override
    public void run() {
        while (true) {
            IORequest request = requestQueue.poll();
            FileHandle handle = request.getFileHandle();
            FileChannel channel = handle.getFileChannel();
            IORequest.RequestType type = request.getType();
            long offset = request.getOffset();

            IIORequest.INotificationCallback callback = request.getCallback();
            ByteBuffer buffer = request.getBuffer();
            try {
                switch (type) {
                    case READ:
                        while (buffer.remaining() > 0) {
                            int len = channel.read(buffer, offset);
                            if (len < 0) {
                                break;
                            }
                            offset += len;
                        }
                        break;

                    case WRITE:
                        while (buffer.remaining() > 0) {
                            int len = channel.write(buffer, offset);
                            if (len < 0) {
                                break;
                            }
                            offset += len;
                        }
                        break;

                    case CLOSE:
                        handle.close();
                        break;
                }
                if (callback != null) {
                    callback.success(request);
                }
            } catch (IOException e) {
                e.printStackTrace();
                if (callback != null) {
                    callback.failure(request, e);
                }
            }
            ioMgr.free(request);
        }
    }
}