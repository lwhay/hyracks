

# Introduction #
Pregelix is an open-source implementation of the [Pregel](http://googleresearch.blogspot.com/2009/06/large-scale-graph-computing-at-google.html) API. However, Pregelix is built on top of the Hyracks general-purpose query execution engine, instead of from the scratch. In this document, we will guide you to implement and execute your first example application.

# Programming model #
## Vertex ##
The programming model in Pregelix is vertex-oriented. Application developers should implement the **_compute_** method in their implementation subclasses of [Vertex](https://code.google.com/p/hyracks/source/browse/pregelix/pregelix-api/src/main/java/edu/uci/ics/pregelix/api/graph/Vertex.java?name=fullstack-0.2.6), which specifies how a vertex is updated with incoming messages and what should be sent out as the outgoing messages for the next iteration.  Here are our example vertex implementations: [PageRank](https://code.google.com/p/hyracks/source/browse/pregelix/pregelix-example/src/main/java/edu/uci/ics/pregelix/example/PageRankVertex.java?name=fullstack-0.2.6),
[ShortestPaths](https://code.google.com/p/hyracks/source/browse/pregelix/pregelix-example/src/main/java/edu/uci/ics/pregelix/example/ShortestPathsVertex.java?name=fullstack-0.2.6),
[ConnectComponents](https://code.google.com/p/hyracks/source/browse/pregelix/pregelix-example/src/main/java/edu/uci/ics/pregelix/example/ConnectedComponentsVertex.java?name=fullstack-0.2.6),
[Reachability Query](https://code.google.com/p/hyracks/source/browse/pregelix/pregelix-example/src/main/java/edu/uci/ics/pregelix/example/ReachabilityVertex.java?name=fullstack-0.2.6),
[Triangle Counting](https://code.google.com/p/hyracks/source/browse/?name=fullstack-0.2.6#git%2Fpregelix%2Fpregelix-example%2Fsrc%2Fmain%2Fjava%2Fedu%2Fuci%2Fics%2Fpregelix%2Fexample%2Ftrianglecounting),
[Maximal Cliques](https://code.google.com/p/hyracks/source/browse/?name=fullstack-0.2.6#git%2Fpregelix%2Fpregelix-example%2Fsrc%2Fmain%2Fjava%2Fedu%2Fuci%2Fics%2Fpregelix%2Fexample%2Fmaximalclique),
[Graph Mutation](https://code.google.com/p/hyracks/source/browse/pregelix/pregelix-example/src/main/java/edu/uci/ics/pregelix/example/GraphMutationVertex.java?name=fullstack-0.2.6).

## Message Combiner ##
Besides the **Vertex** implementation, we advertise users to provide the [MessageCombiner](https://code.google.com/p/hyracks/source/browse/pregelix/pregelix-api/src/main/java/edu/uci/ics/pregelix/api/graph/MessageCombiner.java?name=fullstack-0.2.6) implementations as well, so as to compress the messages at an early stage and reduce unnecessary network traffics.  Note that this is crucial for performance, especially when the graph is skewed. [Here](https://code.google.com/p/hyracks/source/browse/pregelix/pregelix-api/src/main/java/edu/uci/ics/pregelix/api/util/DefaultMessageCombiner.java?name=fullstack-0.2.6) is an example implementation of the MessageCombiner, which listifies all messages for a receiver.

## Global Aggregator ##
Pregelix provides the support for global aggregate. If one wants to have the global aggregate in the application, s/he needs to provide an implementation class of [GlobalAggregator](https://code.google.com/p/hyracks/source/browse/pregelix/pregelix-api/src/main/java/edu/uci/ics/pregelix/api/graph/GlobalAggregator.java?name=fullstack-0.2.6).  [Here](https://code.google.com/p/hyracks/source/browse/pregelix/pregelix-api/src/main/java/edu/uci/ics/pregelix/api/util/GlobalCountAggregator.java?name=fullstack-0.2.6) is an example implementation of the GlobalAggregator, which gets the number of live vertices in the graph.

One can call the following static method to access the up-to-date global aggregate value:
```
IterationUtils.readGlobalAggregateValue(Configuration conf, String jobId)
```

## Graph Mutations ##
Pregelix supports updating vertex states, adding vertex, removing vertex, adding edges and remove vertex.

# Implementing the first example #
In this section, we are going to build the neighbor counting example.
This is a very simple example that count the inbound neighbors for each vertex in the graph.


## Development environment ##
1. Install Eclipse:

http://www.eclipse.org/downloads/

2. Install Maven Eclipse plugin:

Maven3 support within Eclipse (Follow the instructions at  http://m2eclipse.sonatype.org/installing-m2eclipse.html).

3. Create a Maven project, by running the following script [create.sh](http://code.google.com/p/hyracks/wiki/CreateSh) ("chmod +x create.sh" also):
```
# enter the workspace first		
create.sh edu.uci.ics.pregelix neighbor-counting		
```

## Example pom ##
Here is an example pom for Pregelix applications: [pom.xml](http://code.google.com/p/hyracks/wiki/PregelixAppPom026)

Use the provided pom.xml to overwrite the existing pom.xml in the neighbor-counting project.

Import the project into Eclipse, by using File-->Import-->Maven-->Existing Maven Modules.


## Code for the Vertex ##
Applications need to implement the Vertex abstract class, and optionally the MessageCombiner and GlobalAggregator class. Currently, we provide 7 example implementations:
[PageRank](https://code.google.com/p/hyracks/source/browse/pregelix/pregelix-example/src/main/java/edu/uci/ics/pregelix/example/PageRankVertex.java?name=fullstack-0.2.6),
[ShortestPaths](https://code.google.com/p/hyracks/source/browse/pregelix/pregelix-example/src/main/java/edu/uci/ics/pregelix/example/ShortestPathsVertex.java?name=fullstack-0.2.6),
[ConnectComponents](https://code.google.com/p/hyracks/source/browse/pregelix/pregelix-example/src/main/java/edu/uci/ics/pregelix/example/ConnectedComponentsVertex.java?name=fullstack-0.2.6),
[Reachability Query](https://code.google.com/p/hyracks/source/browse/pregelix/pregelix-example/src/main/java/edu/uci/ics/pregelix/example/ReachabilityVertex.java?name=fullstack-0.2.6),
[Triangle Counting](https://code.google.com/p/hyracks/source/browse/?name=fullstack-0.2.6#git%2Fpregelix%2Fpregelix-example%2Fsrc%2Fmain%2Fjava%2Fedu%2Fuci%2Fics%2Fpregelix%2Fexample%2Ftrianglecounting),
[Maximal Cliques](https://code.google.com/p/hyracks/source/browse/?name=fullstack-0.2.6#git%2Fpregelix%2Fpregelix-example%2Fsrc%2Fmain%2Fjava%2Fedu%2Fuci%2Fics%2Fpregelix%2Fexample%2Fmaximalclique),
[Graph Mutation](https://code.google.com/p/hyracks/source/browse/pregelix/pregelix-example/src/main/java/edu/uci/ics/pregelix/example/GraphMutationVertex.java?name=fullstack-0.2.6)

In Eclipse, create a package in your Eclipse project with name **edu.uci.ics.pregelix.nc**, under src/main/java.  Add the [source code](http://code.google.com/p/hyracks/wiki/NeighborCountingExample) of neighbor counting into this package.

## Code for the Entry ##
We have an example where a job is kicked-off:
[Client](https://code.google.com/p/hyracks/source/browse/pregelix/pregelix-example/src/main/java/edu/uci/ics/pregelix/example/client/Client.java?name=fullstack-0.2.6)

In the neighbor counting example, we reuse the Client class. We also reuse the input/output format from the ConnectComponents example. The entry code for the application is in the [NeighborCountingVertex.java](http://code.google.com/p/hyracks/wiki/NeighborCountingExample):
```
public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(NeighborCountingVertex.class.getSimpleName());
	job.setVertexClass(NeighborCountingVertex.class);
	job.setVertexInputFormatClass(TextConnectedComponentsInputFormat.class);
	job.setVertexOutputFormatClass(SimpleConnectedComponentsVertexOutputFormat.class);
        /** to combine messages */
	job.setMessageCombinerClass(NeighborCountingVertex.SumCombiner.class);
        /** to improve the message grouping performance */
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
	Client.run(args, job);
}
```


## Executing the first example ##
Enter the neighbor-counting project directory and build neighbor-counting project:
```
mvn clean package
```

Launch a single node local cluster as described [here](http://code.google.com/p/hyracks/wiki/PregelixUserManual#Running_Pregelix_locally).

Copy the application jar from neighbor-counting/target/neighbor-counting-0.2.3-jar-with-dependencies.jar to pregelix-dist/target/appassembler.

Launch the neighbor counting example by:
```
bin/pregelix neighbor-counting-0.2.6-jar-with-dependencies.jar edu.uci.ics.pregelix.nc.NeighborCountingVertex  -inputpaths data/webmap -outputpath /tmp/nc_result -ip `bin/getip.sh` -port 3099
```

Check the result:
```
cat /tmp/nc_result/part-*
```

Similarly we can run the example distributedly as described [here](http://code.google.com/p/hyracks/wiki/PregelixUserManual#Running_Pregelix_distributedly).