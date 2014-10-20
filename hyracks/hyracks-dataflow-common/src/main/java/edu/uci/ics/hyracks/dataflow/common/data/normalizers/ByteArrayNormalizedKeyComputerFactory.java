package edu.uci.ics.hyracks.dataflow.common.data.normalizers;

import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;

public class ByteArrayNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {
    public static ByteArrayNormalizedKeyComputerFactory INSTANCE = new ByteArrayNormalizedKeyComputerFactory();

    @Override public INormalizedKeyComputer createNormalizedKeyComputer() {
        return new INormalizedKeyComputer() {
            @Override public int normalize(byte[] bytes, int start, int length) {
                int normalizedKey = 0;
                for (int i = 0; i < 3; ++i) {
                    normalizedKey <<= 8;
                    if (i < length) {
                        normalizedKey += bytes[start + i] & 0xff;
                    }
                }
                // last byte, shift 7 instead of 8 to avoid negative number
                normalizedKey <<= 7;
                if (3 < length) {
                    normalizedKey += (bytes[start + 3] & 0xfe) >> 1;
                }
                return normalizedKey;
            }
        };
    }
}
