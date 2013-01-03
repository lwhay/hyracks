package edu.uci.ics.hyracks.imru.example.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

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
        } else {
            // System.out.println("add " + name);
            ZipEntry entry = new ZipEntry(name);
            entry.setTime(file.lastModified());
            zip.putNextEntry(entry);
            copy(file, zip);
            zip.closeEntry();
        }
    }

    public static void createHar(File harFile) throws IOException {
        File file = new File(".classpath");
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

        ByteArrayOutputStream memory = new ByteArrayOutputStream();
        ZipOutputStream zip2 = new ZipOutputStream(memory);
        String p = CreateHar.class.getName().replace('.', '/') + ".class";
        URL url = CreateHar.class.getClassLoader().getResource(p);
        String path = url.getPath();
        path = path.substring(0, path.length() - p.length());
        Logger.getLogger(CreateHar.class.getName()).info(
                "Add " + path + " to HAR");
        add("", new File(path), zip2);
        zip2.finish();

        ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(harFile));
        // add("hyracks-deployment.properties", new File(
        // "src/main/resources/hyracks-deployment.properties"), zip);
        // add("lib/hyracks-imru-0.1.0-SNAPSHOT.jar", new File(
        // "target/appassembler/lib/hyracks-imru-0.1.0-SNAPSHOT.jar"), zip);
        for (String line : new String(buf).split("\n")) {
            if (line.contains("kind=\"lib\"")) {
                line = line.substring(line.indexOf("path=\""));
                line = line.substring(line.indexOf("\"") + 1);
                line = line.substring(0, line.indexOf("\""));
                String name = line.substring(line.lastIndexOf("/") + 1);
                add("lib/" + name, new File(line), zip);
            }
        }
        // add("lib/hyracks-data-std-0.2.3-SNAPSHOT.jar", new File(
        // "target/appassembler/lib/hyracks-data-std-0.2.3-SNAPSHOT.jar"), zip);
        // add("classes", new File("target/classes"), zip);

        ZipEntry entry = new ZipEntry("lib/imru-example.jar");
        entry.setTime(System.currentTimeMillis());
        zip.putNextEntry(entry);
        zip.write(memory.toByteArray());
        zip.closeEntry();
        zip.finish();
    }
}
