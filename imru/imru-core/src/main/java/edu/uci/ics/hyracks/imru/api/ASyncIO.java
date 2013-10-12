/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.imru.api;

import java.util.Iterator;
import java.util.LinkedList;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ASyncIO<Data> {
    private int size = 32;
    private LinkedList<Data> queue = new LinkedList<Data>();
    private Object fullSync = new Object();
    private boolean more = true;

    public ASyncIO() {
        this(1);
    }

    public ASyncIO(int size) {
        this.size = size;
    }

    public void close() throws HyracksDataException {
        more = false;
        synchronized (queue) {
            queue.notifyAll();
        }
    }

    public void add(Data data) throws HyracksDataException {
        if (queue.size() > size) {
            synchronized (fullSync) {
                if (queue.size() > size) {
                    try {
                        fullSync.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        synchronized (queue) {
            queue.addLast(data);
            queue.notifyAll();
        }
    }

    public Iterator<Data> getInput() {
        return new Iterator<Data>() {
            Data data;

            @Override
            public void remove() {
            }

            @Override
            public Data next() {
                if (!hasNext())
                    return null;
                Data data2 = data;
                data = null;
                return data2;
            }

            @Override
            public boolean hasNext() {
                try {
                    if (data == null) {
                        synchronized (queue) {
                            while (queue.size() == 0 && more) {
                                queue.wait();
                            }
                            if (queue.size() > 0)
                                data = queue.removeFirst();
                        }
                        synchronized (fullSync) {
                            fullSync.notifyAll();
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return data != null;
            }
        };
    }
}
