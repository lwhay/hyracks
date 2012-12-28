package edu.uci.ics.hyracks.imru.example.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
                add(name + "/" + f.getName(), f, zip);
        } else {
            System.out.println("add " + name);
            ZipEntry entry = new ZipEntry(name);
            entry.setTime(file.lastModified());
            zip.putNextEntry(entry);
            copy(file, zip);
            zip.closeEntry();
        }
    }

    public static void createHar(File harFile) throws IOException {
        ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(harFile));
        add("hyracks-deployment.properties", new File(
                "conf/hyracks-deployment.properties"), zip);
        add("lib/hyracks-imru-0.1.0-SNAPSHOT.jar", new File(
                "lib/hyracks-imru-0.1.0-SNAPSHOT.jar"), zip);
        add("classes", new File("bin"), zip);
    }
}
