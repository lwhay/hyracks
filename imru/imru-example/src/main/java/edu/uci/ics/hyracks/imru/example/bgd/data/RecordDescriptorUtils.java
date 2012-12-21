package edu.uci.ics.hyracks.imru.example.bgd.data;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class RecordDescriptorUtils {
    public static RecordDescriptor getDummyRecordDescriptor(int numFields) {
        return new RecordDescriptor(new ISerializerDeserializer[numFields]);
    }
}
