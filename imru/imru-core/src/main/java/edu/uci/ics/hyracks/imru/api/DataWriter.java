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

package edu.uci.ics.hyracks.imru.api;

import java.io.IOException;
import java.io.Serializable;
import java.util.Vector;

import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;

public class DataWriter<Data extends Serializable> {
    TupleWriter tupleWriter;
    Vector<Data> vector;

    public DataWriter(TupleWriter tupleWriter) throws IOException {
        this.tupleWriter = tupleWriter;
    }

    public DataWriter(Vector<Data> vector) {
        this.vector = vector;
    }
    
    public DataWriter() {
    }

    public void addData(Data data) throws IOException {
        if (tupleWriter != null) {
            byte[] objectData;
            objectData = JavaSerializationUtils.serialize(data);
            tupleWriter.writeInt(objectData.length);
            tupleWriter.write(objectData);
            tupleWriter.finishField();
            tupleWriter.finishTuple();
        } else {
            vector.add(data);
        }
    }
}
