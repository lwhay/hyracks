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

package edu.uci.ics.hyracks.imru.trainmerge;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.file.IMRUFileSplit;

/**
 * Job interface used to train machine learning model which requires
 * millions of sequential updates.
 * 
 * @author Rui Wang
 * @param <Model>
 *            data model
 */
public interface TrainMergeJob<Model extends Serializable> extends Serializable {
    /**
     * Given a split of data (file or directory) and an initial model,
     * train the model using the data.
     * After reading each data example, perform an update of the model.
     * After a sequence of updates of the model, send the model
     * out to a target node. The target node merge its own model
     * with this node.
     * If the last model sent out is m1, the current model sent out is m2,
     * and distance between m1 and m2 is d, then the maximum distance
     * between every two models in the cluster is bounded within 3d
     * if each node select the target sequentially.
     * TODO: mathematical proof
     * 
     * @param context
     *            contains some useful functions
     * @param input
     *            A split of the data (file or directory)
     * @param model
     *            The model which will be updated simultaneously in train() and merge()
     * @param curNodeId
     *            the id of current node with respect to total number of nodes involved in the training.
     * @param totalNodes
     *            total number of nodes involved in the training.
     * @throws IOException
     */
    public void process(TrainMergeContext context, IMRUFileSplit input,
            Model model, int curNodeId, int totalNodes) throws IOException;

    /**
     * Merge a received model with the current model.
     * Usually computing the average model.
     * 
     * @param context
     *            contains some useful functions
     * @param receivedModel
     *            The model sent from another node
     * @return Merged model, suggest to be the same object as model to save memory
     * @throws IOException
     */
    public void receive(TrainMergeContext context, int sourceParition,
            Serializable receivedObject) throws IOException;
}
