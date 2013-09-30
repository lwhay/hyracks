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

public class GrouperProperty {

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
