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
package edu.uci.ics.hyracks.dataflow.hadoop.data;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public class AbstractClassBasedDelegate<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    private Class<? extends T> klass;
    protected transient T instance;
    private Configuration config;

    public AbstractClassBasedDelegate(Class<? extends T> klass, Configuration conf) {
        this.klass = klass;
        this.config = conf;
        init();
    }

    protected Object readResolve() throws ObjectStreamException {
        init();
        return this;
    }

    private void init() {
        try {
//        	Constructor c = klass.getDeclaredConstructor();
//        	System.out.println("[AbstractClassBasedDelegate][init] klass: " + c.toString());
//        	c.setAccessible(true);
//        	instance = (T) c.newInstance();
        	
        	instance = (T) ReflectionUtils.newInstance(klass, this.config);
        	
        	
//            instance = klass.newInstance();
//        } catch (InstantiationException e) {
//            throw new RuntimeException(e);
//        } catch (IllegalAccessException e) {
//            throw new RuntimeException(e);
//        } catch (NoSuchMethodException e) {
//        	throw new RuntimeException(e);
		} catch (SecurityException e) {
			throw new RuntimeException(e);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(e);
//		} catch (InvocationTargetException e) {
//			Throwable cause = e.getCause();
//			System.out.println("InvocationTargetException occurred. Cause: " + cause.getMessage());
//			throw new RuntimeException(e);
		}
    }
}