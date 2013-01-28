package edu.uci.ics.hyracks.data.std.accessors;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.data.std.AbstractHashFunctionQualityTest;

public class ByteBasedBinaryHashFunctionFamilyTest extends AbstractHashFunctionQualityTest {

    IBinaryHashFunctionFamily murmurHashFamily = ByteBasedBinaryHashFunctionFamily.INSTANCE;

    @Test
    public void IntegerKeyTest() {

        IntegerKeyFamilyTest(murmurHashFamily);

    }

    @Test
    public void ByteArrayTest() {

        ByteArrayFamilyTest(murmurHashFamily);

    }

}
