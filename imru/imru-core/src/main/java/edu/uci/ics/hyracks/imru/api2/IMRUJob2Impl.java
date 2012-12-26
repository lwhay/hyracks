package edu.uci.ics.hyracks.imru.api2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;
import edu.uci.ics.hyracks.imru.api.IModel;

public class IMRUJob2Impl<Model extends IModel, T extends Serializable> extends
        IMRUJob2<Model> {
    IMRUJob<Model, T> job;

    public IMRUJob2Impl(IMRUJob<Model, T> job) {
        this.job = job;
    }

    @Override
    public int getCachedDataFrameSize() {
        return job.getCachedDataFrameSize();
    }

    @Override
    public Model initModel() {
        return job.initModel();
    }

    @Override
    public void map(Iterator<ByteBuffer> input, Model model,
            OutputStream output, int cachedDataFrameSize)
            throws HyracksDataException {
        T object = job.map(input, model, cachedDataFrameSize);
        byte[] objectData;
        try {
            objectData = JavaSerializationUtils.serialize(object);
            output.write(objectData);
            output.close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void parse(IHyracksTaskContext ctx, InputStream in,
            IFrameWriter writer) throws HyracksDataException {
        job.parse(ctx, in, writer);
    }

    @Override
    public void reduce(final IHyracksTaskContext ctx,
            final Iterator<byte[]> input, OutputStream output)
            throws HyracksDataException {
        Iterator<T> iterator = new Iterator<T>() {
            @Override
            public void remove() {
            }

            @Override
            public boolean hasNext() {
                return input.hasNext();
            }

            @Override
            public T next() {
                byte[] objectData = input.next();
                if (objectData == null)
                    return null;
                NCApplicationContext appContext = (NCApplicationContext) ctx
                        .getJobletContext().getApplicationContext();
                try {
                    return (T) appContext.deserialize(objectData);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
        T object = job.reduce(iterator);
        byte[] objectData;
        try {
            objectData = JavaSerializationUtils.serialize(object);
            output.write(objectData);
            output.close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public boolean shouldTerminate(Model model) {
        return job.shouldTerminate(model);
    }

    @Override
    public void update(final IHyracksTaskContext ctx,
            final Iterator<byte[]> input, Model model)
            throws HyracksDataException {
        Iterator<T> iterator = new Iterator<T>() {
            @Override
            public void remove() {
            }

            @Override
            public boolean hasNext() {
                return input.hasNext();
            }

            @Override
            public T next() {
                byte[] objectData = input.next();
                if (objectData == null)
                    return null;
                NCApplicationContext appContext = (NCApplicationContext) ctx
                        .getJobletContext().getApplicationContext();
                try {
                    return (T) appContext.deserialize(objectData);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
        job.update(iterator, model);
    }
}