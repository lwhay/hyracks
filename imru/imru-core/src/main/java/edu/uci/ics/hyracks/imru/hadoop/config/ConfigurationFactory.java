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
package edu.uci.ics.hyracks.imru.hadoop.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.imru.base.IConfigurationFactory;
import edu.uci.ics.hyracks.imru.util.SerDeUtils;

public class ConfigurationFactory implements IConfigurationFactory {
    private static final long serialVersionUID = 1L;
    private boolean hasConf;
    private String hadoopConfPath;

    /**
     * HDFS is not used
     */
    public ConfigurationFactory() {
    }

    /**
     * HDFS is used
     * 
     * @param conf
     */
    public ConfigurationFactory(String hadoopConfPath) {
        this.hadoopConfPath=hadoopConfPath;
        hasConf = hadoopConfPath != null;
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

    @Override
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
