package edu.uci.ics.hyracks.examples.onlineaggregation;

public class HadoopTools {
    public static Object newInstance(String className) throws ClassNotFoundException, InstantiationException,
            IllegalAccessException {
        ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(HadoopTools.class.getClassLoader());
            Class<?> clazz = Class.forName(className, true, HadoopTools.class.getClassLoader());
            return newInstance(clazz);
        } finally {
            Thread.currentThread().setContextClassLoader(ctxCL);
        }
    }

    public static Object newInstance(Class<?> clazz) throws InstantiationException, IllegalAccessException {
        return clazz.newInstance();
    }
}