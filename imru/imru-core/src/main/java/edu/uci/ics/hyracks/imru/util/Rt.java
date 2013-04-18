/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.imru.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class Rt {
    public static boolean showTime = false;

    public static void p(Object object) {
        p((String) ("" + object), new Object[0]);
    }

    public static void np(Object object) {
        System.out.println(object);
    }

    public static void p(String format, Object... args) {
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        StackTraceElement e2 = elements[2];
        for (int i = 1; i < elements.length; i++) {
            if (!Rt.class.getName().equals(elements[i].getClassName())) {
                e2 = elements[i];
                break;
            }
        }
        String line = "(" + e2.getFileName() + ":" + e2.getLineNumber() + ")";
        String info = (showTime ? new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + " " : "") + line;
        synchronized (System.out) {
            System.out.println(info + ": " + String.format(format, args));
        }
    }

    public static void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static boolean bytesEquals(byte[] bs1, byte[] bs2) {
        if (bs1.length != bs2.length)
            return false;
        return bytesEquals(bs1, bs2, bs1.length);
    }

    public static boolean bytesEquals(byte[] bs1, byte[] bs2, int len) {
        for (int i = 0; i < len; i++) {
            if (bs1[i] != bs2[i])
                return false;
        }
        return true;
    }

    public static byte[] read(InputStream inputStream) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] buf = new byte[1024];
        while (true) {
            int len = inputStream.read(buf);
            if (len < 0)
                break;
            outputStream.write(buf, 0, len);
        }
        inputStream.close();
        return outputStream.toByteArray();
    }

    public static byte[] readFileByte(File file) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(file);
        byte[] buf = new byte[(int) file.length()];
        int start = 0;
        while (start < buf.length) {
            int len = fileInputStream.read(buf, start, buf.length - start);
            if (len < 0)
                break;
            start += len;
        }
        fileInputStream.close();
        return buf;
    }

    public static String readFile(File file) throws IOException {
        return new String(read(new FileInputStream(file)));
    }

    public static void write(File file, byte[] bs) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        fileOutputStream.write(bs);
        fileOutputStream.close();
    }

    public static void append(File file, String s) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(file, true);
        fileOutputStream.write(s.getBytes());
        fileOutputStream.close();
    }

    public static void showInputStream(final InputStream is, final StringBuilder sb) {
        new Thread() {
            @Override
            public void run() {
                byte[] bs = new byte[1024];
                try {
                    while (true) {
                        int len = is.read(bs);
                        if (len < 0)
                            break;
                        String s = new String(bs, 0, len);
                        System.out.print(s);
                        if (sb != null)
                            sb.append(s);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }

    public static String runAndShowCommand(String... cmd) throws IOException {
        StringBuilder sb2 = new StringBuilder();
        for (String s : cmd) {
            sb2.append(s + " ");
        }
        System.out.println(sb2);

        Process process = Runtime.getRuntime().exec(cmd);
        StringBuilder sb = new StringBuilder();
        showInputStream(process.getInputStream(), sb);
        showInputStream(process.getErrorStream(), sb);
        try {
            process.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static String runAndShowCommand(String cmd) throws IOException {
        return runAndShowCommand(cmd, null);
    }

    public static String runAndShowCommand(String cmd, File dir) throws IOException {
        Process process = Runtime.getRuntime().exec(cmd, null, dir);
        StringBuilder sb = new StringBuilder();
        showInputStream(process.getInputStream(), sb);
        showInputStream(process.getErrorStream(), sb);
        try {
            process.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static void p(ByteBuffer buffer) {
        Rt.p(Rt.getHex(0, buffer.array(), 0, 256, false));
    }

    public static void p(byte[] bs) {
        Rt.p(Rt.getHex(0, bs, 0, bs.length, false));
    }

    public static String getHex(long address, byte[] bs, int offset, int length, boolean simple) {
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
    
    public static void disableLogging() throws Exception {
        Logger globalLogger = Logger.getLogger("");
        Handler[] handlers = globalLogger.getHandlers();
        for (Handler handler : handlers)
            globalLogger.removeHandler(handler);
        globalLogger.addHandler(new Handler() {
            @Override
            public void publish(LogRecord record) {
                String s = record.getMessage();
                if (s.contains("Exception caught by thread")) {
                    System.err.println(s);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() throws SecurityException {
            }
        });
    }
}
