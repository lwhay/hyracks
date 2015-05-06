package edu.uci.ics.pregelix.nc;

import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.MessageCombiner;
import edu.uci.ics.pregelix.api.graph.MsgList;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.example.ConnectedComponentsVertex.SimpleConnectedComponentsVertexOutputFormat;
import edu.uci.ics.pregelix.example.client.Client;
import edu.uci.ics.pregelix.example.inputformat.TextConnectedComponentsInputFormat;
import edu.uci.ics.pregelix.example.io.VLongWritable;

public class NeighborCountingVertex extends
> Vertex<VLongWritable, VLongWritable, FloatWritable, VLongWritable> {

> /
    * The combiner sums up the messages.
    * 
> public static class SumCombiner extends
> > MessageCombiner<VLongWritable, VLongWritable, VLongWritable> {
> > private VLongWritable agg = new VLongWritable();
> > private MsgList

&lt;VLongWritable&gt;

 msgList;


> @Override
> public void stepPartial(VLongWritable vertexIndex, VLongWritable msg)
> > throws HyracksDataException {
> > agg.set(agg.get() + msg.get());

> }

> @SuppressWarnings({ "unchecked", "rawtypes" })
> @Override
> public void init(MsgList msgList) {
> > this.msgList = msgList;
> > this.agg.set(0);

> }

> @Override
> public VLongWritable finishPartial() {
> > return agg;

> }

> @Override
> public void stepFinal(VLongWritable vertexIndex,
> > VLongWritable partialAggregate) throws HyracksDataException {
> > agg.set(agg.get() + partialAggregate.get());

> }

> @Override
> public MsgList

&lt;VLongWritable&gt;

 finishFinal() {
> > msgList.clear();
> > msgList.add(agg);
> > return msgList;

> }
> }

> private VLongWritable tmpVertexValue = new VLongWritable();
> private VLongWritable outputValue = new VLongWritable(1);

> @Override
> public void compute(Iterator

&lt;VLongWritable&gt;

 msgIterator) {
> > if (getSuperstep() == 1) {
> > > tmpVertexValue.set(0);
> > > setVertexValue(tmpVertexValue);
> > > / send 1 to all outbound neighbors **/
> > > sendMsgToAllEdges(outputValue);

> > } else {
> > > while (msgIterator.hasNext()) {
> > > > VLongWritable msg = msgIterator.next();
> > > > tmpVertexValue.set(getVertexValue().get() + msg.get());
> > > > setVertexValue(tmpVertexValue);

> > > }

> > }
> > voteToHalt();

> }**

> public static void main(String[.md](.md) args) throws Exception {
> > PregelixJob job = new PregelixJob(
> > > NeighborCountingVertex.class.getSimpleName());

> > job.setVertexClass(NeighborCountingVertex.class);
> > job.setVertexInputFormatClass(TextConnectedComponentsInputFormat.class);
> > job.setVertexOutputFormatClass(SimpleConnectedComponentsVertexOutputFormat.class);
> > > / to combine messages **/

> > job.setMessageCombinerClass(NeighborCountingVertex.SumCombiner.class);
> > > /****to improve the message grouping performance**/
> > > job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);

> > Client.run(args, job);

> }

}
  }}}```