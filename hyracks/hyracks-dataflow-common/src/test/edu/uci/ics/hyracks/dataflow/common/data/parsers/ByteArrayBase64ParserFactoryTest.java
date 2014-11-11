package edu.uci.ics.hyracks.dataflow.common.data.parsers;

import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.primitive.ByteArrayPointable;
import junit.framework.TestCase;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;

import static edu.uci.ics.hyracks.dataflow.common.data.parsers.ByteArrayHexParserFactoryTest.subArray;

public class ByteArrayBase64ParserFactoryTest extends TestCase {

    @Test
    public void testParseBase64String() throws HyracksDataException {
        IValueParser parser = ByteArrayBase64ParserFactory.INSTANCE.createValueParser();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(bos);
        String empty = "";

        parser.parse(empty.toCharArray(), 0, empty.length(), outputStream);

        byte[] cache = bos.toByteArray();
        assertTrue(ByteArrayPointable.getLength(cache, 0) == 0);
        assertTrue(DatatypeConverter.printBase64Binary(subArray(cache, 2)).equalsIgnoreCase(empty));

        StringBuilder everyChar = new StringBuilder();
        for (char c = 'a'; c <= 'z'; c++) {
            everyChar.append(c);
        }
        for (char c = 'A'; c <= 'Z'; c++) {
            everyChar.append(c);
        }
        for (char c = '0'; c <= '9'; c++) {
            everyChar.append(c);
        }
        everyChar.append("+/");

        bos.reset();
        parser.parse(everyChar.toString().toCharArray(), 0, everyChar.length(), outputStream);
        cache = bos.toByteArray();
        byte[] answer = DatatypeConverter.parseBase64Binary(everyChar.toString());
        assertTrue(ByteArrayPointable.getLength(cache, 0) == answer.length);
        assertTrue(Arrays.equals(answer, subArray(cache, 2)));

        byte[] maxBytes = new byte[ByteArrayPointable.MAX_LENGTH - 1];
        Arrays.fill(maxBytes, (byte) 0xff);
        String maxString = DatatypeConverter.printBase64Binary(maxBytes);
        bos.reset();
        parser.parse(maxString.toCharArray(), 0, maxString.length(), outputStream);
        cache = bos.toByteArray();
        assertTrue(ByteArrayPointable.getLength(cache, 0) == maxBytes.length);
        assertTrue(Arrays.equals(maxBytes, subArray(cache, 2)));
    }

    void printHexStringToBase64(String hexString){
        byte[] bytes = DatatypeConverter.parseHexBinary(hexString);
        System.out.println(DatatypeConverter.printBase64Binary(bytes));
    }

    @Test
    public void testNothing() {
        printHexStringToBase64("34126a9a8c6e8ea118216c7b1048dda0");
        printHexStringToBase64("46b199c4c7fa55085eea0facd6a3c3b8");
        printHexStringToBase64("4a0450fe08c5421bb342f36e26c67a61");
        printHexStringToBase64("173d7751e6fa210b9494503247045c9e");
        printHexStringToBase64("96892c30078964b56082fd00573673e4");
        printHexStringToBase64("f998bd34cfbe9b742c62e0aa9d06c977");
        printHexStringToBase64("204d482bc66e368b72a85e0691804626");
        printHexStringToBase64("3bde012cc94ce1bdb44ade4e38902072");
        printHexStringToBase64("bb6842ce561ac90e75b033821b34f2bd");
        printHexStringToBase64("881b39c29a960774f53d31762684abcc");
    }
}