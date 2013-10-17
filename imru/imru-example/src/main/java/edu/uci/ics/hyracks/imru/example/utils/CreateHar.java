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
import java.util.HashSet;
import java.util.Vector;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import edu.uci.ics.hyracks.imru.util.Rt;

public class CreateHar {
    public static void copy(InputStream in, OutputStream out)
            throws IOException {
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

    public static void add(String name, File file, ZipOutputStream zip)
            throws IOException {
        if (file.isDirectory()) {
            for (File f : file.listFiles())
                add(
                        name.length() == 0 ? f.getName() : name + "/"
                                + f.getName(), f, zip);
        } else if (name.length() > 0) {
            if ("imru-deployment.properties".equals(name))
                return;
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
                "hyracks-dataflow-std-0.2.3-SNAPSHOT.jar",
                "hyracks-api-0.2.3-SNAPSHOT.jar",
                "hyracks-dataflow-common-0.2.3-SNAPSHOT.jar",
                "hyracks-data-std-0.2.3-SNAPSHOT.jar",
                "hyracks-storage-am-common-0.2.3-SNAPSHOT.jar",
                "hyracks-storage-am-btree-0.2.3-SNAPSHOT.jar",
                "hyracks-control-common-0.2.3-SNAPSHOT.jar",
                "hyracks-control-cc-0.2.3-SNAPSHOT.jar",
                "hyracks-control-nc-0.2.3-SNAPSHOT.jar",
                "hyracks-ipc-0.2.3-SNAPSHOT.jar",
                "hyracks-api-0.2.10-SNAPSHOT.jar",
                "hyracks-control-cc-0.2.10-SNAPSHOT.jar",
                "hyracks-control-common-0.2.10-SNAPSHOT.jar",
                "hyracks-control-nc-0.2.10-SNAPSHOT.jar",
                "hyracks-data-std-0.2.10-SNAPSHOT.jar",
                "hyracks-dataflow-common-0.2.10-SNAPSHOT.jar",
                "hyracks-dataflow-std-0.2.10-SNAPSHOT.jar",
                "hyracks-ec2-0.2.10-SNAPSHOT.jar",
                "hyracks-hdfs-core-0.2.10-SNAPSHOT.jar",
                "hyracks-ipc-0.2.10-SNAPSHOT.jar",
                "hyracks-net-0.2.10-SNAPSHOT.jar",
                "hyracks-server-0.2.10-SNAPSHOT.jar",
                "hyracks-storage-am-btree-0.2.10-SNAPSHOT.jar",
                "hyracks-storage-am-common-0.2.10-SNAPSHOT.jar",
                "junit-4.8.1.jar",
                "json-20090211.jar",
                "httpclient-4.1-alpha2.jar",
                "httpcore-4.1-beta1.jar",
                "commons-logging-1.1.1.jar",
                "commons-codec-1.4.jar",
                "args4j-2.0.12.jar",
                "commons-lang3-3.1.jar",
                "commons-io-1.4.jar",
                "jetty-server-8.0.0.RC0.jar",
                "servlet-api-3.0.20100224.jar",
                "jetty-continuation-8.0.0.RC0.jar",
                "jetty-http-8.0.0.RC0.jar",
                "jetty-io-8.0.0.RC0.jar",
                "jetty-webapp-8.0.0.RC0.jar",
                "jetty-xml-8.0.0.RC0.jar",
                "jetty-util-8.0.0.RC0.jar",
                "jetty-servlet-8.0.0.RC0.jar",
                "jetty-security-8.0.0.RC0.jar",
                "wicket-core-1.5.2.jar",
                "wicket-util-1.5.2.jar",
                "wicket-request-1.5.2.jar",
                "slf4j-api-1.6.1.jar",
                "slf4j-jcl-1.6.3.jar",
                "dcache-client-0.0.1.jar",
                "jetty-client-8.0.0.M0.jar",
                "hyracks-net-0.2.3-SNAPSHOT.jar",
                "httpclient-4.1.1.jar",
                "httpcore-4.1.jar",
                "hyracks-server-0.2.3-SNAPSHOT.jar",
                "aws-java-sdk-1.3.27.jar",
                "jackson-core-asl-1.8.9.jar",
                "jackson-mapper-asl-1.8.9.jar",
                "jsch-0.1.49.jar",
                //the following jars were added on 10/15/2013
                "jline.jar", "scalacheck.jar", "scalap.jar",
                "compress-lzf-0.8.4.jar", "config-0.3.1.jar",
                "log4j-1.2.16.jar", "netty-3.2.7.Final.jar",
                "netty-3.5.3.Final.jar", "snappy-java-1.0.4.1.jar",
                "akka-actor-2.0.3.jar", "akka-remote-2.0.3.jar",
                "akka-slf4j-2.0.3.jar", "akka-zeromq-2.0.3.jar",
                "algebird-core_2.9.2-0.1.8.jar", "asm-all-3.3.1.jar",
                "avro-1.6.3.jar", "avro-ipc-1.6.3.jar",
                "cglib-nodep-2.2.2.jar", "colt-1.2.0.jar",
                "commons-beanutils-1.7.0.jar",
                "commons-beanutils-core-1.8.0.jar",
                "commons-collections-3.2.1.jar",
                "commons-configuration-1.6.jar", "commons-digester-1.8.jar",
                "commons-io-2.1.jar", "commons-lang-2.6.jar",
                "commons-math-2.1.jar", "concurrent-1.3.4.jar",
                "concurrentlinkedhashmap-lru-1.2.jar",
                "dispatch-json_2.9.1-0.8.5.jar", "easymock-3.1.jar",
                "fastutil-6.4.4.jar", "flume-ng-sdk-1.2.0.jar",
                "guava-11.0.1.jar", "h2-lzf-1.0.jar", "hadoop-core-1.0.4.jar",
                "jackson-core-asl-1.0.1.jar", "jackson-core-asl-1.9.3.jar",
                "jackson-mapper-asl-1.0.1.jar", "jackson-mapper-asl-1.9.3.jar",
                "JavaEWAH-0.6.6.jar", "jline-0.9.94.jar", "jna-3.0.9.jar",
                "jnr-constants-0.8.2.jar", "jsr305-1.3.9.jar",
                "junit-3.8.1.jar", "junit-4.8.2.jar",
                "junit-interface-0.8.jar", "kryo-2.20-shaded.jar",
                "kryo-serializers-0.20.jar", "mesos-0.9.0-incubating.jar",
                "mimepull-1.6.jar", "objenesis-1.2.jar", "paranamer-2.3.jar",
                "parboiled-core-1.0.2.jar", "parboiled-scala-1.0.2.jar",
                "protobuf-java-2.4.1.jar", "scalacheck_2.9.2-1.9.jar",
                "scalatest_2.9.2-1.8.jar", "sjson_2.9.1-0.15.jar",
                "slf4j-api-1.6.4.jar", "slf4j-log4j12-1.6.1.jar",
                "spray-base-1.0-M2.1.jar", "spray-can-1.0-M2.1.jar",
                "spray-io-1.0-M2.1.jar", "spray-json_2.9.2-1.1.1.jar",
                "spray-server-1.0-M2.1.jar", "spray-util-1.0-M2.1.jar",
                "test-interface-0.5.jar", "twirl-api_2.9.2-0.5.2.jar",
                "twitter4j-core-3.0.3.jar", "twitter4j-stream-3.0.3.jar",
                "velocity-1.7.jar", "zeromq-scala-binding_2.9.1-0.0.6.jar",
                "zkclient-0.1.jar", "zookeeper-3.3.3.jar",
                "httpclient-4.1.jar", "hyracks-comm-0.2.10-SNAPSHOT.jar", };
        for (String s : ss)
            ignoredJars.add(s);
    }

    public static boolean uploadJarFiles = true;

    public static void createHar(File harFile, boolean withHadoopJar,
            int imruPort, String tempDir) throws IOException {
        ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(harFile));
        HashSet<String> existingEntries = new HashSet<String>();
        String p = CreateHar.class.getName().replace('.', '/') + ".class";
        String string = System.getProperty("java.class.path");
        int userCodeId = 0;
        if (string != null) {
            for (String s : string.split(File.pathSeparator)) {
                if (s.length() == 0)
                    continue;
                if (!uploadJarFiles && s.contains("scala-2.9.2"))
                    continue;
                File dir = new File(s);
                if (dir.isDirectory()) {
                    ByteArrayOutputStream memory = new ByteArrayOutputStream();
                    ZipOutputStream zip2 = new ZipOutputStream(memory);
                    Rt.np("add " + dir.getAbsolutePath());
                    add("", dir, zip2);
                    {
                        //If cc is running in this process
                        System.setProperty("imru.port", "" + imruPort);
                        System.setProperty("imru.tempdir", tempDir);
                        //If cc is running in another process
                        ZipEntry entry = new ZipEntry(
                                "imru-deployment.properties");
                        entry.setTime(System.currentTimeMillis());
                        zip2.putNextEntry(entry);
                        String text = "imru.port=" + imruPort + "\r\n";
                        text += "imru.tempdir=" + tempDir + "\r\n";
                        zip2.write(text.getBytes());
                        zip2.closeEntry();
                    }
                    zip2.finish();
                    ZipEntry entry = new ZipEntry("lib/imru-customer-code"
                            + (userCodeId++) + ".jar");
                    entry.setTime(System.currentTimeMillis());
                    zip.putNextEntry(entry);
                    zip.write(memory.toByteArray());
                    zip.closeEntry();
                } else if (uploadJarFiles) {
                    String name = s;
                    int t = name.lastIndexOf('/');
                    if (t > 0)
                        name = name.substring(t + 1);
                    if (ignoredJars.contains(name))
                        continue;
                    if (!withHadoopJar && "hadoop-core-0.20.2.jar".equals(name))
                        continue;
                    if (new File(s).exists()) {
                        if (!(s.contains("jetty") && s.contains("6.1.14"))
                                && !name.startsWith("scala-")
                                && !name.startsWith("spark-")
                                && !name.startsWith("jsp-")
                                && !name.startsWith("servlet-")) {
                            if (!existingEntries.contains("lib/" + name)) {
                                Rt.np("add " + name);
                                existingEntries.add("lib/" + name);
                                add("lib/" + name, dir, zip);
                            }
                        }
                    }
                }
            }
        }
        zip.finish();
    }

    public static long createJars(boolean withHadoopJar, int imruPort,
            String tempDir, Vector<String> jars, Vector<String> tmpJars)
            throws IOException {
        long length = 0;
        HashSet<String> existingEntries = new HashSet<String>();
        String string = System.getProperty("java.class.path");
        if (string != null) {
            for (String s : string.split(File.pathSeparator)) {
                if (s.length() == 0)
                    continue;
                if (!uploadJarFiles && s.contains("scala-2.9.2"))
                    continue;
                File dir = new File(s);
                if (dir.isDirectory()) {
                    File tmpJarFile = File.createTempFile("imru-customer-code",
                            ".jar");
                    ZipOutputStream zip2 = new ZipOutputStream(
                            new FileOutputStream(tmpJarFile));

                    //                    ByteArrayOutputStream memory = new ByteArrayOutputStream();
                    //                    ZipOutputStream zip2 = new ZipOutputStream(memory);
                    Rt.np("add " + dir.getAbsolutePath());
                    add("", dir, zip2);
                    {
                        //If cc is running in this process
                        System.setProperty("imru.port", "" + imruPort);
                        System.setProperty("imru.tempdir", tempDir);
                        //If cc is running in another process
                        ZipEntry entry = new ZipEntry(
                                "imru-deployment.properties");
                        entry.setTime(System.currentTimeMillis());
                        zip2.putNextEntry(entry);
                        String text = "imru.port=" + imruPort + "\r\n";
                        text += "imru.tempdir=" + tempDir + "\r\n";
                        zip2.write(text.getBytes());
                        zip2.closeEntry();
                    }
                    zip2.finish();
                    zip2.close();
                    length += tmpJarFile.length();
                    tmpJars.add(tmpJarFile.getAbsolutePath());
                    jars.add(tmpJarFile.getAbsolutePath());
                } else if (uploadJarFiles) {
                    String name = s;
                    int t = name.lastIndexOf('/');
                    if (t > 0)
                        name = name.substring(t + 1);
                    if (ignoredJars.contains(name))
                        continue;
                    if (!withHadoopJar && "hadoop-core-0.20.2.jar".equals(name))
                        continue;
                    if (new File(s).exists()) {
                        if (!(s.contains("jetty") && s.contains("6.1.14"))
                                && !name.startsWith("scala-")
                                && !name.startsWith("spark-")
                                && !name.startsWith("jsp-")
                                && !name.startsWith("servlet-")) {
                            if (!existingEntries.contains(name)) {
                                Rt.np("add " + name);
                                existingEntries.add(name);
                                length += dir.length();
                                jars.add(dir.getAbsolutePath());
                            }
                        }
                    }
                }
            }
        }
        return length;
    }
}
