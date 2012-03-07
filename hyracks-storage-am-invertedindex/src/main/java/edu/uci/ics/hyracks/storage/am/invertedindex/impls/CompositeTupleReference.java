/*
 * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.hyracks.storage.am.invertedindex.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * This class combines two ITupleReferences into a single
 * CompositeTupleReference. The first field of each ITupleReference is provided
 * to form the CompositeTupleReference. This class might be generalized to
 * accommodate more ITupleReferences later and to consist of any position of a
 * field and any number of fields from a ITupleReference.
 * 
 * @author kisskys
 * 
 */
public class CompositeTupleReference implements ITupleReference {

	private ITupleReference leftTuple;
	private int leftFieldId;
	private ITupleReference rightTuple;
	private int rightFieldId;

	public void reset(ITupleReference leftTuple, int leftFieldId,
			ITupleReference rightTuple, int rightFieldId) {
		this.leftTuple = leftTuple;
		this.leftFieldId = leftFieldId;
		this.rightTuple = rightTuple;
		this.rightFieldId = rightFieldId;
	}

	@Override
	public int getFieldCount() {
		return 2;
	}

	@Override
	public byte[] getFieldData(int fIdx) {
		if (fIdx == 0) {
			return leftTuple.getFieldData(0);
		} else {
			return rightTuple.getFieldData(0);
		}
	}

	@Override
	public int getFieldStart(int fIdx) {
		if (fIdx == 0) {
			return leftTuple.getFieldStart(0);
		} else {
			return rightTuple.getFieldStart(0);
		}
	}

	@Override
	public int getFieldLength(int fIdx) {
		if (fIdx == 0) {
			return leftTuple.getFieldLength(0);
		} else {
			return rightTuple.getFieldLength(0);
		}
	}

}
