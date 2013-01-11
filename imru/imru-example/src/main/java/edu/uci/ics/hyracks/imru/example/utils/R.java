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

package edu.uci.ics.hyracks.imru.example.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class R {
    public static void p(Object object) {
        p((String)("" + object),new Object[0]);
    }

    public static void p(String format, Object... args) {
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        String line = "(" + elements[3].getFileName() + ":"
                + elements[3].getLineNumber() + ")";
        String info = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                .format(new Date())
                + " " + line;
        synchronized (System.out) {
            System.out.println(info + ": " + String.format(format, args));
        }
    }
}
