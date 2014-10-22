package edu.uci.ics.hyracks.data.std.primitive;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.*;

public class ByteArrayPointable extends AbstractPointable implements IHashable, IComparable {

    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return false;
        }

        @Override
        public int getFixedLength() {
            return 0;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new ByteArrayPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public int compareTo(byte[] bytes, int start, int length) {
        int thislen = getLength(this.bytes, this.start);
        int thatlen = getLength(bytes, start);

        for (int thisId = 0, thatId = 0; thisId < thislen && thatId < thatlen; ++thisId, ++thatId) {
            if (this.bytes[this.start + SIZE_OF_LENGTH + thisId] != bytes[start + SIZE_OF_LENGTH + thatId]) {
                return (0xff & this.bytes[this.start + SIZE_OF_LENGTH + thisId]) - (0xff & bytes[start + SIZE_OF_LENGTH
                        + thatId]);
            }
        }
        return thislen - thatlen;
    }

    @Override
    public int hash() {
        int h = 0;
        int realLength = getLength(bytes, start);
        for (int i = 0; i < realLength; ++i) {
            h = 31 * h + bytes[start + SIZE_OF_LENGTH + i];
        }
        return h;
    }

    public static final int SIZE_OF_LENGTH = 2;

    public static int getLength(byte[] bytes, int offset) {
        return ((0xFF & bytes[offset]) << 8) + (0xFF & bytes[offset + 1]);
    }

    public static void putLength(int length, byte[] bytes, int offset) {
        bytes[offset] = (byte) ((length >>> 8) & 0xFF);
        bytes[offset + 1] = (byte) ((length >>> 0) & 0xFF);
    }

}
