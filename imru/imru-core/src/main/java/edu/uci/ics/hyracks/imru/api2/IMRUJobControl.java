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

package edu.uci.ics.hyracks.imru.api2;

import java.io.File;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.imru.api.IModel;
import edu.uci.ics.hyracks.imru.base.IJobFactory;
import edu.uci.ics.hyracks.imru.hadoop.config.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.jobgen.GenericAggregationIMRUJobFactory;
import edu.uci.ics.hyracks.imru.jobgen.NAryAggregationIMRUJobFactory;
import edu.uci.ics.hyracks.imru.jobgen.NoAggregationIMRUJobFactory;
import edu.uci.ics.hyracks.imru.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.hyracks.imru.runtime.IMRUDriver;

public class IMRUJobControl<Model extends IModel> {
    public HyracksConnection hcc;
    public ConfigurationFactory confFactory;
    IJobFactory jobFactory;
    IMRUDriver<Model> driver;
    public boolean saveIntermediateModels = true;
    public String modelFileName;
    public boolean useExistingModels;

    public void connect(String ccHost, int ccPort, String hadoopConfPath, String clusterConfPath) throws Exception {
        hcc = new HyracksConnection(ccHost, ccPort);

        if (hadoopConfPath != null && !new File(hadoopConfPath).exists()) {
            System.err.println("Hadoop conf path does not exist!");
            System.exit(-1);
        }
        // Hadoop configuration
        if (clusterConfPath == null || !new File(clusterConfPath).exists())
            ClusterConfig.setConf(hcc);
        else
            ClusterConfig.setConfPath(clusterConfPath);
        if (hadoopConfPath != null) {
            confFactory = new ConfigurationFactory(hadoopConfPath);
        } else {
            confFactory = new ConfigurationFactory();
        }
    }

    public void selectNoAggregation(String examplePaths) {
        jobFactory = new NoAggregationIMRUJobFactory(examplePaths, confFactory);
    }

    public void selectGenericAggregation(String examplePaths, int aggCount) {
        if (aggCount < 1)
            throw new IllegalArgumentException(
                    "Must specify a nonnegative aggregator count using the -agg-count option");
        jobFactory = new GenericAggregationIMRUJobFactory(examplePaths, confFactory, aggCount);
    }

    public void selectNAryAggregation(String examplePaths, int fanIn) {
        if (fanIn < 1) {
            throw new IllegalArgumentException("Must specify nonnegative -fan-in");
        }
        jobFactory = new NAryAggregationIMRUJobFactory(examplePaths, confFactory, fanIn);
    }

    /**
     * run job using low level interface
     * 
     * @param job
     * @param initialModel
     * @param tempPath
     * @param app
     * @return
     * @throws Exception
     */
    public JobStatus run(IIMRUJobSpecificationImpl<Model> job, Model initialModel, String tempPath, String app)
            throws Exception {
        driver = new IMRUDriver<Model>(hcc, job, initialModel, jobFactory, confFactory.createConfiguration(), tempPath,
                app);
        driver.modelFileName = modelFileName;
        driver.saveIntermediateModels = saveIntermediateModels;
        driver.useExistingModels = useExistingModels;
        return driver.run();
    }

    /**
     * run job using middle level interface
     * 
     * @param job2
     * @param tempPath
     * @param app
     * @return
     * @throws Exception
     */
    public JobStatus run(IIMRUJob2<Model> job2, String tempPath, String app) throws Exception {
        Model initialModel = job2.initModel();
        IIMRUJobSpecificationImpl<Model> job = new IIMRUJobSpecificationImpl<Model>(job2);
        return run(job, initialModel, tempPath, app);
    }

    /**
     * run job using high level interface
     * 
     * @param job
     * @param tempPath
     * @param app
     * @return
     * @throws Exception
     */
    public <Data extends Serializable, T extends Serializable> JobStatus run(IIMRUJob<Model, Data, T> job,
            String tempPath, String app) throws Exception {
        return run(new IMRUJob2Impl<Model, Data, T>(job), tempPath, app);
    }

    /**
     * @return The number of iterations performed.
     */
    public int getIterationCount() {
        return driver.getIterationCount();
    }

    /**
     * @return The most recent global model.
     */
    public Model getModel() {
        return driver.getModel();
    }
}
