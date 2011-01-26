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

package edu.uci.ics.hyracks.storage.am.invertedindex.api;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public interface IBinaryTokenizer {

    public void reset(byte[] data, int start, int length);

    public boolean hasNext();

    public void next();

    public int getTokenStartOff();

    public int getTokenLength();

    public int getNumTokens();
    
    public void writeToken(DataOutput dos) throws IOException;

    public RecordDescriptor getTokenSchema();
}
