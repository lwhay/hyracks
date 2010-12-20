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

import java.io.File;
import java.io.IOException;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.DeviceHandle;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IDeviceManager;
import edu.uci.ics.hyracks.api.io.IFileFactory;

public class DeviceManager implements IDeviceManager {
    private final DeviceHandle[] devices;
    private IFileFactory tempFileFactory;

    public DeviceManager(DeviceHandle[] devices) {
        this.devices = devices;
        tempFileFactory = new IFileFactory() {
            private File[] tempFileFolders;
            private int nextDevice;

            {
                tempFileFolders = new File[DeviceManager.this.devices.length];
                for (int i = 0; i < tempFileFolders.length; ++i) {
                    tempFileFolders[i] = new File(DeviceManager.this.devices[i].getPath(), "tmp");
                }
                nextDevice = 0;
            }

            @Override
            public FileReference createFile(String prefix, String suffix) throws HyracksDataException {
                int devId = nextDevice++;
                File tempFolder = tempFileFolders[devId];
                try {
                    return new FileReference(DeviceManager.this.devices[devId], File.createTempFile(prefix, suffix,
                            tempFolder));
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }

    public DeviceHandle[] getDevices() {
        return devices;
    }

    @Override
    public IFileFactory getTempFileFactory() {
        return tempFileFactory;
    }
}