package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface ILinearizerSearchPredicate extends ISearchPredicate {
    public ILinearizerSearchHelper getLinearizerSearchModifier() throws HyracksDataException;
}
