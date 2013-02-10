/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.imru.base;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * Factory for creating Hadoop Configuration objects, because they
 * are not Serializable.
 */
public interface IConfigurationFactory extends Serializable {

    /**
     * @return A new Hadoop Configuration.
     * @throws HyracksDataException
     */
    public Configuration createConfiguration() throws HyracksDataException;

    public InputStream getInputStream(String path) throws IOException;

    public OutputStream getOutputStream(String path) throws IOException;

    public boolean exists(String path) throws IOException;
}
