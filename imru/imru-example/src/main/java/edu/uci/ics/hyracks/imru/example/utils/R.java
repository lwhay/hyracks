package edu.uci.ics.hyracks.imru.example.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class R {
    public static void p(String format, Object... args) {
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        String line = "(" + elements[2].getFileName() + ":"
                + elements[2].getLineNumber() + ")";
        String info = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                .format(new Date())
                + " " + line;
        synchronized (System.out) {
            System.out.println(info + ": " + String.format(format, args));
        }
    }

    public static void printHex(long address, byte[] bs, String format,
            Object... args) {
        System.out.println(String.format(format, args));
        System.out.println(getHex(address, bs, bs.length));
    }

    public static void printHex(long address, byte[] bs) {
        printHex(address, bs, bs.length);
    }

    public static void printHex(long address, byte[] bs, int length) {
        System.out.println(getHex(address, bs, length));
    }

    public static void printHex(long address, byte[] bs, int offset, int length) {
        System.out.println(getHex(address, bs, offset, length));
    }

    public static String getHex(long address, byte[] bs, int length) {
        return getHex(address, bs, 0, length);
    }

    public static String getHex(long address, byte[] bs, int offset, int length) {
        return getHex(address, bs, offset, length, false);
    }

    public static String getHex(long address, byte[] bs, int offset,
            int length, boolean simple) {
        if (length > bs.length - offset)
            length = bs.length - offset;
        int col = 16;
        int row = (length - 1) / col + 1;
        int len = 0;
        for (long t = address; t > 0; t >>= 4) {
            len++;
        }
        len++;
        StringBuilder sb = new StringBuilder();
        for (int y = 0; y < row; y++) {
            if (simple)
                sb.append(String.format("%04x ", address + y * col));
            else
                sb.append(String.format("%" + len + "x:  ", address + y * col));
            for (int x = 0; x < col; x++) {
                if (!simple && x > 0 && x % 4 == 0)
                    sb.append("- ");
                int index = y * col + x;
                if (index < length)
                    sb.append(String.format("%02X ", bs[offset + index]));
                else
                    sb.append("   ");
            }
            if (!simple) {
                sb.append(" ");
                for (int x = 0; x < col; x++) {
                    // if (x > 0 && x % 4 == 0)
                    // System.out.print(" - ");
                    char c;
                    int index = y * col + x;
                    if (index < length)
                        c = (char) bs[offset + index];
                    else
                        c = ' ';
                    if (c < 32 || c >= 127)
                        c = '.';
                    sb.append(c);
                }
            }
            sb.append("\n");
        }
        return sb.toString();
    }
}