/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.storage.common.file;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface ILocalResourceRepository {

    public LocalResource getResourceById(long id) throws HyracksDataException;

    public LocalResource getResourceByName(String name) throws HyracksDataException;

    public void insert(LocalResource resource, int ioDeviceId) throws HyracksDataException;

    public void deleteResourceById(long id, int ioDeviceId) throws HyracksDataException;

    public void deleteResourceByName(String name, int ioDeviceId) throws HyracksDataException;

    public List<LocalResource> getAllResources() throws HyracksDataException;
}
