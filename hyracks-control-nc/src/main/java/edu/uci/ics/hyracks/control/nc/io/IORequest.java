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

import edu.uci.ics.hyracks.api.io.FileHandle;
import edu.uci.ics.hyracks.api.io.IIORequest;

public final class IORequest implements IIORequest {
    private ByteBuffer buffer;
    private RequestType type;
    private FileHandle fHandle;
    private long offset;
    private INotificationCallback callback;

    public IORequest(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setRequest(RequestType type, FileHandle fHandle, long offset, INotificationCallback callback) {
        this.type = type;
        this.fHandle = fHandle;
        this.offset = offset;
        this.callback = callback;
    }

    @Override
    public RequestType getType() {
        return type;
    }

    @Override
    public FileHandle getFileHandle() {
        return fHandle;
    }

    @Override
    public long getOffset() {
        return offset;
    }

    public INotificationCallback getCallback() {
        return callback;
    }
}