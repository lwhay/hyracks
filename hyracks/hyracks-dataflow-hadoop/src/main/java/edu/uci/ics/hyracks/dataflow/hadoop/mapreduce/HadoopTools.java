/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.hadoop.mapreduce;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class HadoopTools {
    public static Object newInstance(String className) throws ClassNotFoundException, InstantiationException,
            IllegalAccessException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException {
        ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(HadoopTools.class.getClassLoader());
            Class<?> clazz = Class.forName(className, true, HadoopTools.class.getClassLoader());
            return newInstance(clazz);
        } finally {
            Thread.currentThread().setContextClassLoader(ctxCL);
        }
    }

    public static Object newInstance(Class<?>clazz) throws InstantiationException, IllegalAccessException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException {
    	Constructor<?> c = clazz.getDeclaredConstructor();
    	c.setAccessible(true);
    	return c.newInstance();
    }
    
    public static <T> T createInstance(Class<? extends T> klass) {
        Constructor<? extends T> c = null;
        try {
            c = klass.getDeclaredConstructor();
            c.setAccessible(true);
            
//            T inst = c.newInstance();
//            if( inst instanceof WritableComparator)
//                return (T) WritableComparator.get(klass.asSubclass(WritableComparable.class));
//            else
            
            return c.newInstance();
        } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            e.printStackTrace();
        }     
        return null;
    }
}