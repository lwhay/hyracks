package edu.uci.ics.hyracks.imru.api2;

import java.io.IOException;
import java.io.Serializable;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;

public class DataWriter<Data extends Serializable> {
    TupleWriter tupleWriter;

    public DataWriter(TupleWriter tupleWriter) throws IOException {
        this.tupleWriter = tupleWriter;
    }

    public void addData(Data data) throws IOException {
        byte[] objectData;
        objectData = JavaSerializationUtils.serialize(data);
        tupleWriter.writeInt(objectData.length);
        tupleWriter.write(objectData);
        tupleWriter.finishField();
        tupleWriter.finishTuple();
    }
}
