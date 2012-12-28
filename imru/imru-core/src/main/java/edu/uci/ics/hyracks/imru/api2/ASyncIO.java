package edu.uci.ics.hyracks.imru.api2;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ASyncIO<Data> {
    private LinkedList<Data> queue = new LinkedList<Data>();
    private boolean more = true;

    public ASyncIO() {
    }

    public void close() throws HyracksDataException {
        more = false;
        synchronized (queue) {
            queue.notifyAll();
        }
    }

    public void add(Data data) throws HyracksDataException {
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
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return data != null;
            }
        };
    }
}
