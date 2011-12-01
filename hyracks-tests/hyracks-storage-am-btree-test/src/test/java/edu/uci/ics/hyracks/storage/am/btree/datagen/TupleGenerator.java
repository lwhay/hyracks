package edu.uci.ics.hyracks.storage.am.btree.datagen;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

@SuppressWarnings({"rawtypes", "unchecked" })
public class TupleGenerator {    
    protected final ISerializerDeserializer[] fieldSerdes;
    protected final IFieldValueGenerator[] fieldGens;
    protected final ArrayTupleBuilder tb;
    protected final ArrayTupleReference tuple;
    protected final byte[] payload;
    protected final Random rnd;
    protected final boolean sorted;
    protected final DataOutput tbDos;
    
    public TupleGenerator(ISerializerDeserializer[] fieldSerdes, int payloadSize, Random rnd, boolean sorted) {
        this.fieldSerdes = fieldSerdes;
        this.rnd = rnd;
        fieldGens = new IFieldValueGenerator[fieldSerdes.length];
        for (int i = 0; i < fieldSerdes.length; i++) {
            fieldGens[i] = getFieldGenFromSerde(fieldSerdes[i]);
        }
        tuple = new ArrayTupleReference();
        if (payloadSize > 0) {
            tb = new ArrayTupleBuilder(fieldSerdes.length + 1);
            payload = new byte[payloadSize];
        } else {
            tb = new ArrayTupleBuilder(fieldSerdes.length);
            payload = null;
        }        
        this.sorted = sorted;
        tbDos = tb.getDataOutput();
    }

    public ITupleReference next() throws IOException {
        tb.reset();
        for (int i = 0; i < fieldSerdes.length; i++) {
            fieldSerdes[i].serialize(fieldGens[i].next(), tbDos);
            tb.addFieldEndOffset();
        }
        if (payload != null) {
            tbDos.write(payload);
            tb.addFieldEndOffset();
        }
        tuple.reset(tb.getFieldEndOffsets(), tb.getByteArray());
        return tuple;
    }
    
    public IFieldValueGenerator getFieldGenFromSerde(ISerializerDeserializer serde) {
        if (serde instanceof IntegerSerializerDeserializer) {
            if (sorted) {
                return new SortedIntegerFieldValueGenerator();
            } else {
                return new IntegerFieldValueGenerator(rnd);
            }
        }
        System.out.println("NULL");
        //if (serde instanceof Integer64SerializerDeserializer) {
        //    throw new UnsupportedOperationException("Binary comparator factory for Integer64 not implemented.");
        //}
        //if (serde instanceof FloatSerializerDeserializer) {
        //    return FloatBinaryComparatorFactory.INSTANCE;
        //}
        //if (serde instanceof DoubleSerializerDeserializer) {
        //    return DoubleBinaryComparatorFactory.INSTANCE;
        //}
        return null;
    }
}
