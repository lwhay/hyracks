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
        for (int thisId = 0, thatId = 0; thisId < this.length && thatId < length; ++thisId, ++thatId) {
            if (this.bytes[this.start + thisId] != bytes[start + thatId]) {
                return 0xff & this.bytes[this.start + thisId] - 0xff & bytes[start + thatId];
            }
        }
        return this.length - length;
    }

    @Override
    public int hash() {
        int h = 0;
        for (int i = 0; i < length; ++i) {
            h = 31 * h + bytes[start + i];
        }
        return h;
    }
}
