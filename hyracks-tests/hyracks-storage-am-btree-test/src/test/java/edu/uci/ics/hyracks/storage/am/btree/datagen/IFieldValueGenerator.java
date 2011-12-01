package edu.uci.ics.hyracks.storage.am.btree.datagen;

public interface IFieldValueGenerator<T> {
    public T next();
}
