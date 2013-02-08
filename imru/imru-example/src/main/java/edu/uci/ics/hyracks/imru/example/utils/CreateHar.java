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

package edu.uci.ics.hyracks.imru.example.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import edu.uci.ics.hyracks.imru.util.R;

public class CreateHar {
    public static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] bs = new byte[1024];
        while (true) {
            int len = in.read(bs);
            if (len <= 0)
                break;
            out.write(bs, 0, len);
        }
    }

    public static void copy(File file, OutputStream out) throws IOException {
        FileInputStream input = new FileInputStream(file);
        copy(input, out);
        input.close();
    }

    public static void add(String name, File file, ZipOutputStream zip) throws IOException {
        if (file.isDirectory()) {
            for (File f : file.listFiles())
                add(name.length() == 0 ? f.getName() : name + "/" + f.getName(), f, zip);
        } else if (name.length() > 0) {
            ZipEntry entry = new ZipEntry(name);
            entry.setTime(file.lastModified());
            zip.putNextEntry(entry);
            copy(file, zip);
            zip.closeEntry();
        }
    }

    static HashSet<String> ignoredJars = new HashSet<String>();
    static {
        //These jars are contained by hyracks server, there is no need to update them
        String[] ss = {
//                "hadoop-core-0.20.2.jar",
                "commons-cli-1.2.jar",
                "xmlenc-0.52.jar",
                "commons-httpclient-3.0.1.jar",
                "commons-net-1.4.1.jar",
                "jasper-runtime-5.5.12.jar",
                "jasper-compiler-5.5.12.jar",
                "ant-1.6.5.jar",
                "commons-el-1.0.jar",
                "jets3t-0.7.1.jar",
                "kfs-0.3.jar",
                "hsqldb-1.8.0.10.jar",
                "oro-2.0.8.jar",
                "core-3.1.1.jar",
                "hadoop-test-0.20.2.jar",
                "ftplet-api-1.0.0.jar",
                "mina-core-2.0.0-M5.jar",
                "ftpserver-core-1.0.0.jar",
                "ftpserver-deprecated-1.0.0-M2.jar",
                "javax.servlet-api-3.0.1.jar",
                //                "hyracks-dataflow-std-0.2.3-SNAPSHOT.jar", 
                "hyracks-api-0.2.3-SNAPSHOT.jar",
                //                "hyracks-dataflow-common-0.2.3-SNAPSHOT.jar",
                //                "hyracks-data-std-0.2.3-SNAPSHOT.jar",
                //                "hyracks-storage-am-common-0.2.3-SNAPSHOT.jar",
                //                "hyracks-storage-am-btree-0.2.3-SNAPSHOT.jar", 
                "hyracks-control-common-0.2.3-SNAPSHOT.jar", "hyracks-control-cc-0.2.3-SNAPSHOT.jar",
                "hyracks-control-nc-0.2.3-SNAPSHOT.jar", "hyracks-ipc-0.2.3-SNAPSHOT.jar", "junit-4.8.1.jar",
                "json-20090211.jar", "httpclient-4.1-alpha2.jar", "httpcore-4.1-beta1.jar",
                "commons-logging-1.1.1.jar", "commons-codec-1.4.jar", "args4j-2.0.12.jar", "commons-lang3-3.1.jar",
                "commons-io-1.4.jar", "jetty-server-8.0.0.RC0.jar", "servlet-api-3.0.20100224.jar",
                "jetty-continuation-8.0.0.RC0.jar", "jetty-http-8.0.0.RC0.jar", "jetty-io-8.0.0.RC0.jar",
                "jetty-webapp-8.0.0.RC0.jar", "jetty-xml-8.0.0.RC0.jar", "jetty-util-8.0.0.RC0.jar",
                "jetty-servlet-8.0.0.RC0.jar", "jetty-security-8.0.0.RC0.jar", "wicket-core-1.5.2.jar",
                "wicket-util-1.5.2.jar", "wicket-request-1.5.2.jar", "slf4j-api-1.6.1.jar", "slf4j-jcl-1.6.3.jar",
                "dcache-client-0.0.1.jar", "jetty-client-8.0.0.M0.jar", "hyracks-net-0.2.3-SNAPSHOT.jar", };
        for (String s : ss)
            ignoredJars.add(s);
    }

    public static void createHar(File harFile) throws IOException {
        File classPathFile = new File(".classpath");
        boolean startedFromEclipse = classPathFile.exists();
        ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(harFile));
        String p = CreateHar.class.getName().replace('.', '/') + ".class";
        URL url = CreateHar.class.getClassLoader().getResource(p);
        String path = url.getPath();
        if (false) {
            path = path.substring(0, path.length() - p.length());
            Logger.getLogger(CreateHar.class.getName()).info("Add " + path + " to HAR");
            ByteArrayOutputStream memory = new ByteArrayOutputStream();
            ZipOutputStream zip2 = new ZipOutputStream(memory);
            add("", new File(path), zip2);
            zip2.finish();
            ZipEntry entry = new ZipEntry("lib/imru-example.jar");
            entry.setTime(System.currentTimeMillis());
            zip.putNextEntry(entry);
            zip.write(memory.toByteArray());
            zip.closeEntry();
        }
        if (false && startedFromEclipse) {

            FileInputStream fileInputStream = new FileInputStream(classPathFile);
            byte[] buf = new byte[(int) classPathFile.length()];
            int start = 0;
            while (start < buf.length) {
                int len = fileInputStream.read(buf, start, buf.length - start);
                if (len < 0)
                    break;
                start += len;
            }
            fileInputStream.close();
            for (String line : new String(buf).split("\n")) {
                if (line.contains("kind=\"lib\"")) {
                    line = line.substring(line.indexOf("path=\""));
                    line = line.substring(line.indexOf("\"") + 1);
                    line = line.substring(0, line.indexOf("\""));
                    String name = line.substring(line.lastIndexOf("/") + 1);
                    add("lib/" + name, new File(line), zip);
                }
            }
        } else {
            String string = System.getProperty("java.class.path");
            ByteArrayOutputStream memory = new ByteArrayOutputStream();
            ZipOutputStream zip2 = null;
            if (string != null) {
                for (String s : string.split(File.pathSeparator)) {
                    if (s.length() == 0)
                        continue;
                    File dir = new File(s);
                    if (dir.isDirectory()) {
                        if (zip2 == null)
                            zip2 = new ZipOutputStream(memory);
//                        R.np("add " + dir.getAbsolutePath());
                        add("", dir, zip2);
                    } else {
                        String name = s;
                        int t = name.lastIndexOf('/');
                        if (t > 0)
                            name = name.substring(t + 1);
                        if (ignoredJars.contains(name))
                            continue;
                        if (new File(s).exists()) {
                            if (!(s.contains("jetty") && s.contains("6.1.14"))) {
//                                R.np("add " + name);
                                add("lib/" + name, dir, zip);
                            }
                        }
                    }
                }
            }
            if (zip2 != null) {
                zip2.finish();
                ZipEntry entry = new ZipEntry("lib/imru-customer-code.jar");
                entry.setTime(System.currentTimeMillis());
                zip.putNextEntry(entry);
                zip.write(memory.toByteArray());
                zip.closeEntry();
            }
        }
        zip.finish();
    }
}
