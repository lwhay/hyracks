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

public class IMRUJobControl<Model extends IModel, T extends Serializable> {
    public HyracksConnection hcc;
    public Configuration conf = new Configuration();
    public ConfigurationFactory confFactory;
    IJobFactory jobFactory;
    IMRUDriver<Model> driver;

    public void connect(String ccHost, int ccPort, String hadoopConfPath,
            String clusterConfPath) throws Exception {
        hcc = new HyracksConnection(ccHost, ccPort);

        if (!new File(hadoopConfPath).exists()) {
            System.err.println("Hadoop conf path does not exist!");
            System.exit(-1);
        }
        // Hadoop configuration
        conf.addResource(new Path(hadoopConfPath + "/core-site.xml"));
        conf.addResource(new Path(hadoopConfPath + "/mapred-site.xml"));
        conf.addResource(new Path(hadoopConfPath + "/hdfs-site.xml"));
        ClusterConfig.setConfPath(clusterConfPath);
        confFactory = new ConfigurationFactory(conf);
    }

    public void selectNoAggregation(String examplePaths) {
        jobFactory = new NoAggregationIMRUJobFactory(examplePaths, confFactory);
    }

    public void selectGenericAggregation(String examplePaths, int aggCount) {
        if (aggCount < 1)
            throw new IllegalArgumentException(
                    "Must specify a nonnegative aggregator count using the -agg-count option");
        jobFactory = new GenericAggregationIMRUJobFactory(examplePaths,
                confFactory, aggCount);
    }

    public void selectNAryAggregation(String examplePaths, int fanIn) {
        if (fanIn < 1) {
            throw new IllegalArgumentException(
                    "Must specify nonnegative -fan-in");
        }
        jobFactory = new NAryAggregationIMRUJobFactory(examplePaths,
                confFactory, fanIn);
    }

    public JobStatus run2(IMRUJob2<Model> job2, Model initialModel,
            String tempPath, String app) throws Exception {
        IIMRUJobSpecificationImpl<Model> job = new IIMRUJobSpecificationImpl<Model>(
                job2);
        driver = new IMRUDriver<Model>(hcc, job, initialModel, jobFactory,
                conf, tempPath, app);
        return driver.run();
    }

    public JobStatus run(final IMRUJob<Model, T> job, String tempPath,
            String app) throws Exception {
        Model initialModel = job.initModel();
        IIMRUJobSpecificationImpl<Model> job2 = new IIMRUJobSpecificationImpl<Model>(
                new IMRUJob2Impl<Model, T>(job));
        driver = new IMRUDriver<Model>(hcc, job2, initialModel, jobFactory,
                conf, tempPath, app);
        return driver.run();
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
