package edu.uci.ics.hyracks.data.std.accessors;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.data.std.AbstractHashFunctionQualityTest;

public class MurmurHash3BinaryHashFunctionFamilyTest extends AbstractHashFunctionQualityTest {

    IBinaryHashFunctionFamily murmurHashFamily = MurmurHash3BinaryHashFunctionFamily.INSTANCE;

    @Test
    public void IntegerKeyTest() {

        IntegerKeyFamilyTest(murmurHashFamily);

    }

    @Test
    public void ByteArrayTest() {

        ByteArrayFamilyTest(murmurHashFamily);

    }

}
