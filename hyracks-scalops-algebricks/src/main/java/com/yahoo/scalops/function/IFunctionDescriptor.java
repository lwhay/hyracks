package com.yahoo.scalops.function;

public interface IFunctionDescriptor {

    public Object getReturnType();
    
    public IClojureEvaluator createClojure();
    
    public String getName();
    
}
