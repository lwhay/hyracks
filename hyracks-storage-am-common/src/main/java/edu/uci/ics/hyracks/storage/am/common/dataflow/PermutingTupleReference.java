package edu.uci.ics.hyracks.storage.am.common.dataflow;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class PermutingTupleReference implements ITupleReference {

    private byte[] tupleData;
    private int[] fieldPermutation;
    private int[] fEndOffsets;

    public void reset(int[] fEndOffsets, int[] fieldPermutation, byte[] tupleData) {
        this.fEndOffsets = fEndOffsets;
        this.fieldPermutation = fieldPermutation;
        this.tupleData = tupleData;
    }

    @Override
    public int getFieldCount() {
        return fieldPermutation.length;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return tupleData;
    }

    @Override
    public int getFieldStart(int fIdx) {
        return (fieldPermutation[fIdx] == 0) ? 0 : fEndOffsets[fieldPermutation[fIdx - 1]];
    }

    @Override
    public int getFieldLength(int fIdx) {
        return (fieldPermutation[fIdx] == 0) ? fEndOffsets[0] : fEndOffsets[fieldPermutation[fIdx]]
                - fEndOffsets[fieldPermutation[fIdx - 1]];
    }
}
