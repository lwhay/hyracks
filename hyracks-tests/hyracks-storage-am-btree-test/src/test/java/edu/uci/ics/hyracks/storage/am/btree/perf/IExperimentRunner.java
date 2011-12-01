package edu.uci.ics.hyracks.storage.am.btree.perf;

import edu.uci.ics.hyracks.storage.am.btree.datagen.DataGenThread;

public interface IExperimentRunner {
    public void init() throws Exception;
    
    public long runExperiment(DataGenThread dataGen) throws Exception;
    
    public void deinit() throws Exception;
}
