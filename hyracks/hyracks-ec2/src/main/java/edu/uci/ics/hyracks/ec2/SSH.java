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
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

import javax.swing.JOptionPane;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelShell;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.UIKeyboardInteractive;
import com.jcraft.jsch.UserInfo;

/**
 * Wrapper of jsch
 * 
 * @author wangrui
 */
public class SSH implements Runnable {
    public class MyUserInfo implements UserInfo, UIKeyboardInteractive {
        public String getPassword() {
            throw new Error();
        }

        public boolean promptYesNo(String str) {
            //                        R.p(str + "yes");
            return true;
        }

        public String getPassphrase() {
            throw new Error();
        }

        public boolean promptPassphrase(String message) {
            throw new Error();
        }

        public boolean promptPassword(String message) {
            throw new Error();
        }

        public void showMessage(String message) {
            throw new Error();
        }

        public String[] promptKeyboardInteractive(String destination, String name, String instruction, String[] prompt,
                boolean[] echo) {
            throw new Error();
        }
    }

    JSch jsch = new JSch();
    Session session;
    ChannelShell shell;
    ChannelSftp ftp;
    PrintStream out;
    private boolean exitFlag = false;

    public SSH(String user, String ip, int port, File pemFile) throws Exception {
        if (pemFile != null)
            jsch.addIdentity(pemFile.getAbsolutePath());
        session = jsch.getSession(user, ip, port);
        MyUserInfo ui = new MyUserInfo();
        session.setUserInfo((UserInfo) ui);
        session.connect();
        shell = (ChannelShell) session.openChannel("shell");
        shell.connect();
        ftp = (ChannelSftp) session.openChannel("sftp");
        ftp.connect();
        out = new PrintStream(shell.getOutputStream());
        new Thread(this).start();
    }

    public InputStream get(String path) throws SftpException {
        return ftp.get(path);
    }

    public void cat(String path) throws SftpException, IOException {
        try {
            InputStream in = ftp.get(path);
            byte[] bs = new byte[1024];
            while (true) {
                int len = in.read(bs);
                if (len < 0)
                    break;
                System.out.print(new String(bs, 0, len));
            }
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void upload(File localDir, String remoteDir) throws SftpException, IOException {
        if (localDir.isDirectory()) {
            execute("mkdir -p " + remoteDir);
            for (File file : localDir.listFiles()) {
                upload(file, remoteDir + "/" + file.getName());
            }
        } else {
            SftpATTRS attr = null;
            try {
                attr = ftp.lstat(remoteDir);
            } catch (Exception e) {
            }
            if (attr == null || localDir.lastModified() > attr.getMTime() * 1000L
                    || attr.getSize() != localDir.length()) {
                Rt.np("uploading " + localDir);
                ftp.put(localDir.getAbsolutePath(), remoteDir);
            } else {
                Rt.np("skip " + localDir);
            }
        }
    }

    public void put(String path, InputStream in) throws SftpException, IOException {
        ftp.put(in, path);
        in.close();
    }

    public void close() {
        exitFlag = true;
        ftp.disconnect();
        shell.disconnect();
        session.disconnect();
    }

    String cmd = null;
    String result = null;
    private boolean executed = false;
    StringBuilder input;
    public static int timeout = 0;

    public synchronized String execute(String cmd) {
        return execute(cmd, false);
    }

    public synchronized String execute(String cmd, boolean needInput) {
        result = null;
        if (needInput)
            input = new StringBuilder();
        else
            input = null;
        this.cmd = cmd;
        long start = System.currentTimeMillis();
        while (result == null) {
            Rt.sleep(50);
            if (timeout > 0) {
                if (System.currentTimeMillis() > start + timeout) {
                    out.write(3);
                    out.flush();
                    throw new Error("timeout");
                }
            }
        }
        // R.np("["+cmd+"] finished");
        if (input != null)
            return input.toString();
        return null;
    }

    static String formatLine(String line) {
        StringBuilder sb = new StringBuilder();
        boolean escaping = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == 0x1B) {
                //http://www2.gar.no/glinkj/help/cmds/ansa.htm
                //http://vt100.net/docs/vt510-rm/SGR
                escaping = true;
            } else if (escaping && (c == 'm' || c == 'K')) {
                escaping = false;
            } else if (!escaping) {
                sb.append(line.charAt(i));
            }
        }
        return sb.toString();
    }

    public String readLineNoWait(InputStream in) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while (in.available() > 0) {
            int t = in.read();
            if (t < 0)
                break;
            out.write(t);
            if (t == '\n')
                break;
        }
        if (out.size() == 0)
            return null;
        return new String(out.toByteArray());
    }

    @Override
    public void run() {
        try {
            InputStream in = shell.getInputStream();
            while (!exitFlag) {
                String line = readLineNoWait(in);
                if (line == null) {
                    Thread.sleep(20);
                    continue;
                }
                line = formatLine(line);
                if (input != null) {
                    synchronized (input) {
                        input.append(line);
                    }
                }
                if (line.endsWith("$ ") || line.endsWith("# ")) {
                    Rt.np(line);
                    if (executed) {
                        executed = false;
                        result = "";
                    }
                    // R.np("wait cmd");
                    while (!exitFlag && cmd == null)
                        Rt.sleep(50);
                    if (exitFlag)
                        return;
                    executed = true;
                    // R.np("cmd " + cmd);
                    out.println(cmd);
                    cmd = null;
                    out.flush();
                    Rt.sleep(50);
                } else {
                    System.out.print(line);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
