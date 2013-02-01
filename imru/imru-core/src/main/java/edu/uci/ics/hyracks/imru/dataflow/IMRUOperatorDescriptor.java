package edu.uci.ics.hyracks.imru.dataflow;

import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.imru.api.IIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.api.IModel;
import edu.uci.ics.hyracks.imru.base.IConfigurationFactory;

abstract public class IMRUOperatorDescriptor<Model extends IModel> extends AbstractSingleActivityOperatorDescriptor {
    protected final IIMRUJobSpecification<Model> imruSpec;
    protected final IConfigurationFactory confFactory;

    public IMRUOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputArity, int outputArity, String name,
            IIMRUJobSpecification<Model> imruSpec, IConfigurationFactory confFactory) {
        super(spec, inputArity, outputArity);
        this.setDisplayName(name);
        this.imruSpec = imruSpec;
        this.confFactory = confFactory;
    }
}
