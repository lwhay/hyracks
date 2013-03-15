package edu.uci.ics.hyracks.imru.dataflow;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.imru.api.IIMRUJob2;
import edu.uci.ics.hyracks.imru.file.ConfigurationFactory;

abstract public class IMRUOperatorDescriptor<Model extends Serializable>
        extends AbstractSingleActivityOperatorDescriptor {
    protected final IIMRUJob2<Model> imruSpec;

    public IMRUOperatorDescriptor(IOperatorDescriptorRegistry spec,
            int inputArity, int outputArity, String name,
            IIMRUJob2<Model> imruSpec) {
        super(spec, inputArity, outputArity);
        this.setDisplayName(name);
        this.imruSpec = imruSpec;
    }
}
