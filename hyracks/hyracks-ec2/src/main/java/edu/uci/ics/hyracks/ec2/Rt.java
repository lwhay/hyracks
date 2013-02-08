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

package edu.uci.ics.hyracks.ec2;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A list of useful functions
 * 
 * @author wangrui
 *
 */
public class Rt {
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
        String info = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + " " + line;
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

    public static String readFile(File file) throws IOException {
        return new String(read(new FileInputStream(file)));
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
}
