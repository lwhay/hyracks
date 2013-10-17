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

package edu.uci.ics.hyracks.imru.api;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.imru.file.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.jobgen.ClusterConfig;
import edu.uci.ics.hyracks.imru.jobgen.IMRUJobFactory;
import edu.uci.ics.hyracks.imru.runtime.IMRUDriver;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;

public class IMRUJobControl<Model extends Serializable, Data extends Serializable> {
    public HyracksConnection hcc;
    public IMRUConnection imruConnection;
    public ConfigurationFactory confFactory;
    IMRUJobFactory jobFactory;
    IMRUDriver<Model, Data> driver;
    public String localIntermediateModelPath;
    public String modelFileName;
    public boolean memCache = false;
    public boolean noDiskCache = false;
    public int frameSize;

    public void connect(String ccHost, int ccPort, int imruPort,
            String hadoopConfPath, String clusterConfPath) throws Exception {
        hcc = new HyracksConnection(ccHost, ccPort);
        imruConnection = new IMRUConnection(ccHost, imruPort);

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

    public void selectNoAggregation(String examplePaths) throws IOException,
            InterruptedException {
        jobFactory = new IMRUJobFactory(imruConnection, examplePaths,
                confFactory, IMRUJobFactory.AGGREGATION.NONE, 0, 0);
    }

    public void selectGenericAggregation(String examplePaths, int aggCount)
            throws IOException, InterruptedException {
        if (aggCount < 1)
            throw new IllegalArgumentException(
                    "Must specify a nonnegative aggregator count using the -agg-count option");
        jobFactory = new IMRUJobFactory(imruConnection, examplePaths,
                confFactory, IMRUJobFactory.AGGREGATION.GENERIC, 0, aggCount);
    }

    public void selectNAryAggregation(String examplePaths, int fanIn)
            throws IOException, InterruptedException {
        if (fanIn < 1) {
            throw new IllegalArgumentException(
                    "Must specify nonnegative -fan-in");
        }
        jobFactory = new IMRUJobFactory(imruConnection, examplePaths,
                confFactory, IMRUJobFactory.AGGREGATION.NARY, fanIn, 0);
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
    public JobStatus run(DeploymentId deploymentId,
            IIMRUJob2<Model, Data> job2, Model initialModel, String app)
            throws Exception {
        //        Model initialModel = job2.initModel();
        driver = new IMRUDriver<Model, Data>(hcc, deploymentId, imruConnection,
                job2, initialModel, jobFactory, confFactory
                        .createConfiguration(), app);
        driver.memCache = memCache;
        driver.noDiskCache = noDiskCache;
        driver.modelFileName = modelFileName;
        driver.frameSize = frameSize;
        driver.localIntermediateModelPath = localIntermediateModelPath;
        return driver.run();
    }

    public JobStatus generateData(DeploymentId deploymentId,
            IIMRUDataGenerator generator, String app) throws Exception {
        driver = new IMRUDriver<Model, Data>(hcc, deploymentId, imruConnection,
                null, null, jobFactory, confFactory.createConfiguration(), app);
        driver.memCache = memCache;
        driver.noDiskCache = noDiskCache;
        driver.modelFileName = modelFileName;
        driver.localIntermediateModelPath = localIntermediateModelPath;
        return driver.runDataGenerator(generator);
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
    public <T extends Serializable> JobStatus run(DeploymentId deploymentId,
            IIMRUJob<Model, Data, T> job, Model initialModel, String app)
            throws Exception {
        return run(deploymentId, new IMRUJob2Impl<Model, Data, T>(deploymentId,
                job), initialModel, app);
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
