package edu.uci.ics.hyracks.imru.example.bgd.job;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.imru.api.IOneByOneUpdateFunction;
import edu.uci.ics.hyracks.imru.api.IUpdateFunction;
import edu.uci.ics.hyracks.imru.api.IUpdateFunctionFactory;
import edu.uci.ics.hyracks.imru.example.bgd.data.FragmentableFloatArray;
import edu.uci.ics.hyracks.imru.example.bgd.data.LinearModel;
import edu.uci.ics.hyracks.imru.example.bgd.data.RecordDescriptorUtils;

public class BGDUpdateFunctionFactory implements IUpdateFunctionFactory<LinearModel> {

    @Override
    public IUpdateFunction createUpdateFunction(final IHyracksTaskContext ctx, final LinearModel model) {
        return new IOneByOneUpdateFunction() {

            private IFrameTupleAccessor accessor;
            private boolean readLoss;
            private float loss;
            private FragmentableFloatArray gradientAcc;

            @Override
            public void open() throws HyracksDataException {
                // Accessor to read loss frame
                accessor = new FrameTupleAccessor(ctx.getFrameSize(), RecordDescriptorUtils.getDummyRecordDescriptor(1));
                gradientAcc = new FragmentableFloatArray(new float[model.numFeatures]);
                loss = 0.0f;
            }

            @Override
            public void close() throws HyracksDataException {
                // Update loss
                model.loss = loss;
                model.loss += model.regularizationConstant * norm(model.weights.array);
                // Update weights
                for (int i = 0; i < model.weights.length; i++) {
                    model.weights.array[i] = (model.weights.array[i] - gradientAcc.array[i] * model.stepSize)
                            * (1.0f - model.stepSize * model.regularizationConstant);
                }
                model.stepSize *= 0.9;
                model.roundsRemaining--;
            }

            @Override
            public void update(ByteBuffer chunk) throws HyracksDataException {
                if (!readLoss) {
                    accessor = new FrameTupleAccessor(ctx.getFrameSize(), RecordDescriptorUtils.getDummyRecordDescriptor(1));
                    accessor.reset(chunk);
                    loss += FloatPointable.getFloat(
                            chunk.array(),
                            accessor.getTupleStartOffset(0) + accessor.getFieldSlotsLength()
                                    + accessor.getFieldStartOffset(0, 0));
                    accessor = new FrameTupleAccessor(ctx.getFrameSize(), RecordDescriptorUtils.getDummyRecordDescriptor(2));
                    readLoss = true;
                } else {
                    accessor.reset(chunk);
                    for (int i = 0; i < accessor.getTupleCount(); i++) {
                        FragmentableFloatArray gradientFragment = FragmentableFloatArray.readFromFrame(accessor, i);
                        gradientAcc.accumulate(gradientFragment);
                    }
                }
            }
        };
    }

    /**
     * @return The Euclidean norm of the vector.
     */
    public static double norm(float[] vec) {
        double norm = 0.0;
        for (double comp : vec) {
            norm += comp * comp;
        }
        return Math.sqrt(norm);
    }

}
