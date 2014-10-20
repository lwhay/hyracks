package edu.uci.ics.hyracks.dataflow.common.data.marshalling;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ByteArraySerializerDeserializer implements ISerializerDeserializer<byte[]> {

    private static final long serialVersionUID = 1L;

    public final static ByteArraySerializerDeserializer INSTANCE = new ByteArraySerializerDeserializer();
    public final static int MAX_LENGTH = 65536;

    private ByteArraySerializerDeserializer() {
    }

    @Override
    public byte[] deserialize(DataInput in) throws HyracksDataException {
        try {
            int length = in.readUnsignedShort();
            byte[] bytes = new byte[length];
            in.readFully(bytes, 0, length);
            return bytes;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(byte[] instance, DataOutput out) throws HyracksDataException {

        if (instance.length >= MAX_LENGTH) {
            throw new HyracksDataException(
                    "encoded byte array too long: " + instance.length + " bytes");
        }
        try {
            out.writeShort(instance.length);
            out.write(instance, 0, instance.length);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public static final int SIZE_OF_LENGTH = 2;

    public static int getLength(byte [] bytes, int offset){
        return (( 0xFF & bytes[offset]) << 8) + (0xFF & bytes[offset+1]);
    }

    public static void putLength(int length, byte [] bytes, int offset){
        bytes[offset] = (byte) ((length >>> 8) & 0xFF);
        bytes[offset+1] = (byte) ((length >>> 0) & 0xFF);
    }

}
