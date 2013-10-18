/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.group.global.costmodels;

/**
 * This class describes the properties those should be considered when designing a aggregation plan.
 */
public class GrouperProperty {

    /**
     * The properties to be considered for the output of a group-by algorithm. The following properties are included:<br/>
     * - <b>SORTED</b>: whether the output of the algorithm is sorted on the group-by condition. If the output is
     * sorted, then an algorithms utilizing the sorted order should be picked. <br/>
     * - <b>AGGREGATED</b>: whether the output of the algorithm contains no duplicates on the group-by condition. This
     * property can be used to describe the output cardinality, and also the status of the aggregation. <br/>
     * - <b>GLOBALLY_PARTITIONED</b>: whether the output of the algorithm contains all records from the input for value
     * of the group-by condition. This property describes whether the input data has been fully partitioned. Note that
     * if the output is both SORTEd and GLOBALLY_PARTITIONED, the output is the final aggregation result.
     * <p/>
     */
    public enum Property {
        SORTED,
        AGGREGATED,
        GLOBALLY_PARTITIONED
    };

    short grouperProperty;

    public GrouperProperty() {
        grouperProperty = (short) 0;
    }

    public GrouperProperty(Property... props) {
        grouperProperty = (short) 0;
        for (Property prop : props) {
            setProperty(prop);
        }
    }

    public String toString() {
        return (hasProperty(Property.SORTED) ? "SORTED, " : "")
                + (hasProperty(Property.AGGREGATED) ? "AGGREGATED, " : "")
                + (hasProperty(Property.GLOBALLY_PARTITIONED) ? "GLOBALLY_PARTITIONED, " : "");
    }

    public GrouperProperty createCopy() {
        GrouperProperty copy = new GrouperProperty();
        copy.grouperProperty = this.grouperProperty;
        return copy;
    }

    public boolean hasProperty(Property prop) {
        return ((grouperProperty >> prop.ordinal()) & 1) == 1;
    }

    public void setProperty(Property prop) {
        grouperProperty = (short) (grouperProperty | (1 << prop.ordinal()));
    }

    public void setProperty(Property prop, boolean value) {
        if (value) {
            setProperty(prop);
        } else {
            if (hasProperty(prop)) {
                grouperProperty = (short) (grouperProperty ^ (1 << prop.ordinal()));
            }
        }
    }

    public boolean isAggregationDone() {
        return hasProperty(Property.AGGREGATED) && hasProperty(Property.GLOBALLY_PARTITIONED);
    }

    /**
     * Check whether this grouper property is compatible with the given grouper property mask. "Compatible" here means
     * that if some property is required by the maskProp, this group property should have that property. Otherwise, it
     * does not matter whether this group property has that or not.
     * 
     * @param maskProp
     *            The mask property representing the required properties.
     * @return
     */
    public boolean isCompatibleWithMask(GrouperProperty maskProp) {
        for (Property prop : Property.values()) {
            if (maskProp.hasProperty(prop) && !this.hasProperty(prop)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return grouperProperty;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof GrouperProperty)) {
            return false;
        }
        return grouperProperty == ((GrouperProperty) obj).grouperProperty;
    }
}
