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

package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;

public class PrefixMergePolicy implements ILSMMergePolicy {

    private static final Logger LOGGER = Logger.getLogger(PrefixMergePolicy.class.getName());
    private long maxMergableComponentSize;
    private int maxToleranceComponentCount;

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested) throws HyracksDataException,
            IndexException {
        // 1.  Look at the candidate components for merging in oldest-first order.  If one exists, identify the prefix of the sequence of
        // all such components for which the sum of their sizes exceeds MaxMrgCompSz.  Schedule a merge of those components into a new component.
        // 2.  If a merge from 1 doesn't happen, see if the set of candidate components for merging exceeds MaxTolCompCnt.  If so, schedule
        // a merge all of the current candidates into a new single component.
        List<ILSMComponent> immutableComponents = new ArrayList<ILSMComponent>(index.getImmutableComponents());
        List<ILSMComponent> mergableComponents = new ArrayList<ILSMComponent>();
        
//        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
//        SimpleDateFormat formatter = new SimpleDateFormat(format);
//        formatter.setTimeZone(TimeZone.getTimeZone("PDT"));
//        formatter.setLenient(false);
//        
//        
//        Date now = new Date();
//        String strDate = formatter.format(now);
//        
//        Date date = null;
//        try {
//            date = formatter.parse(strDate);
//        } catch (ParseException e) {
//
//            e.printStackTrace();
//        }
//        long datetimeInMillis = date.getTime();
//        
//        
//        long duration = 100000L;
//        long minInterestingTime = datetimeInMillis - duration;
//        int k = 0;
//        
//        if (immutableComponents.get(k).getLSMComponentFilter() != null) {
//            for (; k < immutableComponents.size(); k++) {
//                LOGGER.severe("XXXX: "
//                        + Integer64SerializerDeserializer.getLong(immutableComponents.get(k).getLSMComponentFilter()
//                                .getMinTuple().getFieldData(0), immutableComponents.get(k).getLSMComponentFilter()
//                                .getMinTuple().getFieldStart(0) + 1) + " XXXXX " + minInterestingTime);
//                if (Integer64SerializerDeserializer.getLong(immutableComponents.get(k).getLSMComponentFilter()
//                        .getMinTuple().getFieldData(0), immutableComponents.get(k).getLSMComponentFilter()
//                        .getMinTuple().getFieldStart(0) + 1) > minInterestingTime) {
//                    mergableComponents.add(immutableComponents.get(k));
//                } else {
//                    break;
//                }
//            }
//            if (mergableComponents.size() > 1) {
//                ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
//                        NoOpOperationCallback.INSTANCE);
//                accessor.scheduleMerge(index.getIOOperationCallback(), mergableComponents);
//                LOGGER.severe("YYYY: " + k);
//                return;
//            } else {
//                mergableComponents.clear();
//            }
//        }
        
//      String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
//      SimpleDateFormat formatter = new SimpleDateFormat(format);
//      formatter.setTimeZone(TimeZone.getTimeZone("PDT"));
//      formatter.setLenient(false);
//      
//      
//      Date now = new Date();
//      String strDate = formatter.format(now);
//      
//      Date date = null;
//      try {
//          date = formatter.parse(strDate);
//      } catch (ParseException e) {
//
//          e.printStackTrace();
//      }
//      long datetimeInMillis = date.getTime();
//      
//      
//      long duration = 100000L;
//      long minInterestingTime = datetimeInMillis - duration;
//      int k = 0;
//      
//      if (immutableComponents.get(k).getLSMComponentFilter() != null) {
//          for (; k < immutableComponents.size(); k++) {
//              LOGGER.severe("XXXX: "
//                      + Integer64SerializerDeserializer.getLong(immutableComponents.get(k).getLSMComponentFilter()
//                              .getMinTuple().getFieldData(0), immutableComponents.get(k).getLSMComponentFilter()
//                              .getMinTuple().getFieldStart(0) + 1) + " XXXXX " + minInterestingTime);
//              if (Integer64SerializerDeserializer.getLong(immutableComponents.get(k).getLSMComponentFilter()
//                      .getMinTuple().getFieldData(0), immutableComponents.get(k).getLSMComponentFilter()
//                      .getMinTuple().getFieldStart(0) + 1) > minInterestingTime) {
//                  immutableComponents.remove(k);
//              } else {
//                  break;
//              }
//          }
//      }

        // Reverse the components order so that we look at components from oldest to newest.
        Collections.reverse(immutableComponents);

        for (ILSMComponent c : immutableComponents) {
            if (c.getState() != ComponentState.READABLE_UNWRITABLE) {
                return;
            }
        }
        if (fullMergeIsRequested) {
            LOGGER.severe("Full compact for index: [" + index + "] has been scheduled");
            ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            accessor.scheduleFullMerge(index.getIOOperationCallback());
            return;
        }
        long totalSize = 0;
        int startIndex = -1;
        for (int i = 0; i < immutableComponents.size(); i++) {
            ILSMComponent c = immutableComponents.get(i);
            long componentSize = ((AbstractDiskLSMComponent) c).getComponentSize();
            if (componentSize > maxMergableComponentSize) {
                startIndex = i;
                totalSize = 0;
                continue;
            }
            totalSize += componentSize;
            boolean isLastComponent = i + 1 == immutableComponents.size() ? true : false;
            if (totalSize > maxMergableComponentSize
                    || (isLastComponent && i - startIndex >= maxToleranceComponentCount)) {
                for (int j = startIndex + 1; j <= i; j++) {
                    mergableComponents.add(immutableComponents.get(j));
                }
                // Reverse the components order back to its original order
                Collections.reverse(mergableComponents);
                ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                        NoOpOperationCallback.INSTANCE);
                accessor.scheduleMerge(index.getIOOperationCallback(), mergableComponents);
                LOGGER.severe("ZZZZZZZZZZZ");
                break;
            }
        }
    }

    @Override
    public void configure(Map<String, String> properties) {
        maxMergableComponentSize = Long.parseLong(properties.get("max-mergable-component-size"));
        maxToleranceComponentCount = Integer.parseInt(properties.get("max-tolerance-component-count"));
    }
}
