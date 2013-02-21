/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.hyracks.imru.runtime.bootstrap;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.application.ICCBootstrap;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * The bootstrap class of the application that will manage its life cycle at the
 * Cluster Controller.
 */
public class IMRUCCBootstrapImpl implements ICCBootstrap {
    private static final Logger LOGGER = Logger
            .getLogger(IMRUCCBootstrapImpl.class.getName());
    private Server webServer;
    private ICCApplicationContext appCtx;
    private Hashtable<String, String> jobStatus = new Hashtable<String, String>();

    @Override
    public void start() throws Exception {
        LOGGER.info("Starting IMRU model uploader/downloader");
        try {
            setupWebServer();
            webServer.start();
        } catch (java.net.BindException e) {
            Rt.p(e.getMessage());
        }
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Stopping IMRU model uploader/downloader");

        webServer.stop();
    }

    @Override
    public void setApplicationContext(ICCApplicationContext appCtx) {
        this.appCtx = appCtx;
    }

    int port = 3288;
    String modelDir;

    private void setupWebServer() throws Exception {
        InputStream stream = this.getClass().getClassLoader()
                .getResourceAsStream("imru-deployment.properties");
        Properties p = new Properties();
        p.load(stream);
        stream.close();
        String portStr = p.getProperty("imru.port");
        if (System.getProperty("imru.port") != null)
            portStr = System.getProperty("imru.port");
        port = Integer.parseInt(portStr);
        modelDir = p.getProperty("imru.tempdir");
        if (System.getProperty("imru.tempdir") != null)
            modelDir = System.getProperty("imru.tempdir");
        webServer = new Server(port);

        ServletContextHandler context = new ServletContextHandler(
                ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        webServer.setHandler(context);
        context.addServlet(new ServletHolder(new Servlet() {
            @Override
            public void service(ServletRequest request, ServletResponse response)
                    throws ServletException, IOException {
                HttpServletRequest r = (HttpServletRequest) request;
                String key = r.getParameter("key");
                String name = r.getParameter("name");
                LOGGER.info(r.getRequestURI() + " " + name);
                if (name != null && name.contains("/"))
                    throw new IOException(name);
                if ("/put".equals(r.getRequestURI())) {
                    byte[] bs = Rt.read(r.getInputStream());
                    File dir = new File(modelDir);
                    if (!dir.exists())
                        dir.mkdirs();
                    File file = new File(dir, name);
                    Rt.write(file, bs);
                    File keyfile = new File(dir, name + ".accesskey");
                    Rt.write(keyfile, key.getBytes());
                    PrintWriter pw = response.getWriter();
                    pw.println("ok");
                    pw.close();
                } else if ("/get".equals(r.getRequestURI())) {
                    File dir = new File(modelDir);
                    if (!dir.exists())
                        throw new IOException(dir.getAbsolutePath());
                    File keyfile = new File(dir, name + ".accesskey");
                    if (!keyfile.exists())
                        throw new IOException(keyfile.getAbsolutePath());
                    if (!Rt.readFile(keyfile).equals(key))
                        throw new IOException("access denied");
                    File file = new File(dir, name);
                    if (!file.exists())
                        throw new IOException(file.getAbsolutePath());
                    byte[] bs = Rt.readFileByte(file);
                    OutputStream pw = response.getOutputStream();
                    pw.write(bs);
                    pw.close();
                } else if ("/setStatus".equals(r.getRequestURI())) {
                    String jobName = request.getParameter("jobName");
                    String status = request.getParameter("status");
                    if (jobName != null && status != null)
                        jobStatus.put(jobName, status);
                } else if ("/getStatus".equals(r.getRequestURI())) {
                    String jobName = request.getParameter("jobName");
                    PrintWriter pw = response.getWriter();
                    if (jobName != null) {
                        String status = jobStatus.get(jobName);
                        if (status != null)
                            pw.print(status);
                    }
                    pw.close();
                } else if ("/finishJob".equals(r.getRequestURI())) {
                    String jobName = request.getParameter("jobName");
                    if (jobName != null)
                        jobStatus.remove(jobName);
                }
            }

            @Override
            public void init(ServletConfig arg0) throws ServletException {

            }

            @Override
            public String getServletInfo() {
                return null;
            }

            @Override
            public ServletConfig getServletConfig() {
                return null;
            }

            @Override
            public void destroy() {
            }
        }), "/*");
    }
}