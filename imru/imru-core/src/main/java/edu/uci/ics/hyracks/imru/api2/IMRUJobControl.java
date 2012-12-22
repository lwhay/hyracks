package edu.uci.ics.hyracks.imru.api2;

import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.imru.api.IModel;
import edu.uci.ics.hyracks.imru.base.IJobFactory;
import edu.uci.ics.hyracks.imru.deserialized.AbstractDeserializingIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedMapFunction;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedMapFunctionFactory;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedReduceFunction;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedReduceFunctionFactory;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedUpdateFunction;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedUpdateFunctionFactory;
import edu.uci.ics.hyracks.imru.hadoop.config.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.hyracks.imru.runtime.IMRUDriver;

public class IMRUJobControl<Model extends IModel, T extends Serializable> {
    public HyracksConnection hcc;
    public Configuration conf = new Configuration();
    public ConfigurationFactory confFactory;

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

    IMRUDriver<Model> driver;

    static class JobSpecification<Model extends IModel, T extends Serializable>
            extends AbstractDeserializingIMRUJobSpecification<Model, T> {
        IMRUJob2<Model, T> job2;

        public JobSpecification(IMRUJob2<Model, T> job2) {
            this.job2 = job2;
        }

        @Override
        public ITupleParserFactory getTupleParserFactory() {
            return job2.getTupleParserFactory();
        }

        @Override
        public int getCachedDataFrameSize() {
            return job2.getCachedDataFrameSize();
        }

        public boolean shouldTerminate(Model model) {
            return job2.shouldTerminate(model);
        };

        @Override
        public IDeserializedMapFunctionFactory<T, Model> getDeserializedMapFunctionFactory() {
            return new IDeserializedMapFunctionFactory<T, Model>() {
                @Override
                public IDeserializedMapFunction<T> createMapFunction(
                        final Model model, final int cachedDataFrameSize) {
                    return new IDeserializedMapFunction<T>() {
                        @Override
                        public void open() throws HyracksDataException {
                            job2.openMap(model, cachedDataFrameSize);
                        }

                        @Override
                        public T close() throws HyracksDataException {
                            return job2.closeMap(model, cachedDataFrameSize);
                        }

                        @Override
                        public void map(ByteBuffer input)
                                throws HyracksDataException {
                            job2.map(input, model, cachedDataFrameSize);
                        }
                    };
                }
            };
        }

        @Override
        public IDeserializedReduceFunctionFactory<T> getDeserializedReduceFunctionFactory() {
            return new IDeserializedReduceFunctionFactory<T>() {
                @Override
                public IDeserializedReduceFunction<T> createReduceFunction() {
                    return new IDeserializedReduceFunction<T>() {
                        @Override
                        public void open() throws HyracksDataException {
                            job2.openReduce();
                        }

                        @Override
                        public T close() throws HyracksDataException {
                            return job2.closeReduce();
                        }

                        @Override
                        public void reduce(T input) throws HyracksDataException {
                            job2.reduce(input);
                        }
                    };
                }
            };
        }

        @Override
        public IDeserializedUpdateFunctionFactory<T, Model> getDeserializedUpdateFunctionFactory() {
            return new IDeserializedUpdateFunctionFactory<T, Model>() {
                public IDeserializedUpdateFunction<T> createUpdateFunction(
                        final Model model) {
                    return new IDeserializedUpdateFunction<T>() {
                        @Override
                        public void open() throws HyracksDataException {
                            job2.openUpdate(model);
                        }

                        @Override
                        public void close() throws HyracksDataException {
                            job2.closeUpdate(model);
                        }

                        @Override
                        public void update(T input) throws HyracksDataException {
                            job2.update(input, model);
                        }
                    };
                }
            };
        }
    }

    public JobStatus run(IMRUJob2<Model, T> job2, Model initialModel,
            IJobFactory jobFactory, String tempPath, String app)
            throws Exception {
        JobSpecification<Model, T> job = new JobSpecification<Model, T>(job2);
        driver = new IMRUDriver<Model>(hcc, job, initialModel, jobFactory,
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
