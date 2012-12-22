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
import edu.uci.ics.hyracks.imru.example.bgd2.LinearModel;
import edu.uci.ics.hyracks.imru.hadoop.config.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.hyracks.imru.runtime.IMRUDriver;

public interface IMRUJob2<Model extends IModel, T extends Serializable>
        extends Serializable {
    public abstract int getCachedDataFrameSize();

    public abstract ITupleParserFactory getTupleParserFactory();

    public abstract boolean shouldTerminate(Model model);

    public abstract void openMap(Model model, int cachedDataFrameSize)
            throws HyracksDataException;

    public abstract void map(ByteBuffer input, Model model,
            int cachedDataFrameSize) throws HyracksDataException;

    public abstract T closeMap(Model model, int cachedDataFrameSize)
            throws HyracksDataException;

    public abstract void openReduce() throws HyracksDataException;

    public abstract void reduce(T input) throws HyracksDataException;

    public abstract T closeReduce() throws HyracksDataException;

    public abstract void openUpdate(Model model) throws HyracksDataException;

    public abstract void update(T input, Model model)
            throws HyracksDataException;

    public abstract void closeUpdate(Model model) throws HyracksDataException;
}
