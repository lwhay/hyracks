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
package edu.uci.ics.hyracks.imru.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ConfigurationFactory implements Serializable {
    private static final long serialVersionUID = 1L;
    private boolean hasConf;
    public String hadoopConfPath;

    /**
     * HDFS is not used
     */
    public ConfigurationFactory() {
        hasConf = false;
    }

    /**
     * HDFS is used
     * 
     * @param conf
     */
    public ConfigurationFactory(String hadoopConfPath) {
        this.hadoopConfPath = hadoopConfPath;
        hasConf = hadoopConfPath != null;
    }

    public boolean useHDFS() {
        return hasConf;
    }

    public InputStream getInputStream(String path) throws IOException {
        if (!hasConf) {
            return new FileInputStream(new File(path));
        } else {
            Configuration conf = createConfiguration();
            FileSystem dfs = FileSystem.get(conf);
            return dfs.open(new Path(path));
        }
    }

    public OutputStream getOutputStream(String path) throws IOException {
        if (!hasConf) {
            return new FileOutputStream(new File(path));
        } else {
            Configuration conf = createConfiguration();
            FileSystem dfs = FileSystem.get(conf);
            return dfs.create(new Path(path), true);
        }
    }

    public boolean exists(String path) throws IOException {
        if (!hasConf) {
            return new File(path).exists();
        } else {
            Configuration conf = createConfiguration();
            FileSystem dfs = FileSystem.get(conf);
            return dfs.exists(new Path(path));
        }
    }

    public Configuration createConfiguration() throws HyracksDataException {
        if (!hasConf)
            return null;
        try {
            Configuration conf = new Configuration();
            conf.addResource(new Path(hadoopConfPath + "/core-site.xml"));
            conf.addResource(new Path(hadoopConfPath + "/mapred-site.xml"));
            conf.addResource(new Path(hadoopConfPath + "/hdfs-site.xml"));
            return conf;
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }
}
