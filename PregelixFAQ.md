**Q1**: What is the difference between Pregelix and [Hadoop](http://hadoop.apache.org/) or [Hive](http://hive.apache.org/)?

**A**: Pregelix offers a simple and specialized API for graph jobs and executes those jobs on a highly optimized data-parallel execution engine, Hyracks.  The combination of efficient operators, connectors, index structures make Pregelix an order-of-magnitude faster than Hadoop or Hive, for the PageRank task.

**Q2**: Why do we need to configure Hadoop in order to run Pregelix?

**A**: Pregelix is able to read/write the graph data from any **logically** shared file system (NFS, HDFS, and so on).  Among them, HDFS (Hadoop file system) is the suggested one for permanent storage. Other than that, Pregelix does not use anything in the Hadoop framework.

**Q3**: How is Pregelix compared with the [HaLoop](http://code.google.com/p/haloop) system?

**A**:  HaLoop is a modified version of Hadoop that adds user-specified caching behaviors. However, it is still fundamentally limited by the MapReduce runtime, e.g., no general data-flow support, no index support, forced HDFS roundtrips, and so on.  Therefore, HaLoop got 2X faster than Hadoop on the PageRank task, but Pregelix gets more than 10X faster! On the other hand, Pregelix API is specialized for graph algorithms instead of general iterative ones.

**Q4**: How is Pregelix compared with the [Giraph](http://incubator.apache.org/giraph) system?

**A**: Pregelix supports out-of-core computation while Giraph does not.  Thus, Pregelix offers more elasticity on the hardware configurations. Pregelix is built on-top-of the Hyracks general purpose dataflow engine, such that the development is much easier than building a message passing system like Giraph from scratch. Meanwhile, the highly optimized Hyracks runtime enables the high-performance of Pregelix. Last but not least, Pregelix uses the memory in a more economic way than Giraph.

**Q5**:  I encountered the following exception when running a job:
```
org.apache.hadoop.mapreduce.lib.input.InvalidInputException: Input path does not exist: file:/webmap
	at edu.uci.ics.pregelix.core.driver.Driver.runJob(Driver.java:149)
	at edu.uci.ics.pregelix.example.client.Client.run(Client.java:74)
	at edu.uci.ics.pregelix.example.PageRankVertex.main(PageRankVertex.java:211)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:39)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:25)
	at java.lang.reflect.Method.invoke(Method.java:597)
```

**A**: It is very possible you did not export the right HADOOP\_HOME. You can do the following and then try again, for example
```
export HADOOP_HOME=~/hadoop-0.20.2
```