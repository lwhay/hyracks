


---


# Introduction #
## What is Pregelix? ##
Pregelix is an open-source implementation of the Pregel API for large-scale graph analytics, including several key features:

|Feature|Description|
|:------|:----------|
| **Simple API** |Preglix supports the simple "think like a vertex" API popularized by Google's Pregel and GraphLab systems.|
| **Scalable runtime** |Pregelix is built on top of the Hyracks execution engine, which provides cluster management, task scheduling, <br />network management, and fault-tolerance.|
| **High-performance** |Pregelix under-the-hood is an efficient Hyracks query execution plan, with very efficient runtime operators.|
| **Out-of-core computation** |The data-parallel Hyracks operators that support Pregelix make efficient use of external memory.|
| **Flexible execution strategy** | Pregelix supports several logically equivalent but physically different execution plans.|
| **Hadoop compatible** |Pregelix, like Hyracks, supports HDFS (Hadoop Distributed File System) data connectors hooked into its runtime.<br /> Hence a Pregelix job can be incorporated into an arbitrary legacy Hadoop MapReduce workflow. |
| **Graph mutations** |adding/removing vertex and adding/removing edges are supported.|

Here are two **unique** highlights in Pregelix:

  * Pregelix uses the Hyracks data-parallel execution engine to execute graph analytics jobs. Pregelix benefits from the Hyracks operators and connectors which have been designed to make efficient use of available main memory to produce results quickly.

  * Pregelix has out-of-core support for all tasks, gracefully spilling data from memory to disk when running in memory-constrained situations.  The result is that, unlike some of its peers, Pregelix imposes no minimum machine count or per-machine memory requirement in order to run graph computations without OutOfMemoryErrors.  This can be advantageous when dealing with very large data sets, as system sizing then becomes a performance choice rather than a possible showstopper.

## New features ##
New features in this release:
| **Cache-sensitive optimization** | Pregelix jobs can set an optional normalized key computers to get the standard [Alpha-sort](https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=4&cad=rja&ved=0CEkQFjAD&url=http%3A%2F%2Fresearch.microsoft.com%2F~gray%2FAlphaSort.doc&ei=5ASxUc_dLuOGjAKk1YH4Bg&usg=AFQjCNEh_ZXt0Nqcv7ghuzaibSj8Vw9o9g&sig2=EQOp-ImJbHHUl7IE9tTZKg&bvm=bv.47534661,d.cGE) optimization. <br /> This will dramatically improve the message grouping performance. |
|:---------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Improved job lifecycle management** | The job deployment time is significantly reduced. |
| **Binary distribution** | Users do not need to build the source code from scratch, instead, we offer distribution binaries directly. |

We also fixed a few bugs in the vertex insert/delete/update code path and get things tested at scale.

# Prerequisite #
Supported platforms:
Linux (kernel 2.6.x or above) or Mac (OS X 10.7 or above), either 32bit or 64bit.

Software requirement:
|  **Java 1.7.x or above** |
|:-------------------------|

# Getting Pregelix #
## Download Pregelix ##
| For default HDFS (0.20.2) users | [distribution zip](https://hyracks.googlecode.com/files/pregelix-dist-0.2.6-binary-assembly-hdfs-0.20.2.zip) |
|:--------------------------------|:-------------------------------------------------------------------------------------------------------------|
| For HDFS-0.23.1 users | [distribution zip](https://hyracks.googlecode.com/files/pregelix-dist-0.2.6-binary-assembly-hdfs-0.23.1.zip)  |
| For HDFS-0.23.6 users | [distribution zip](https://hyracks.googlecode.com/files/pregelix-dist-0.2.6-binary-assembly-hdfs-0.23.6.zip)  |
| For HDFS-1.0.4 users | [distribution zip](https://hyracks.googlecode.com/files/pregelix-dist-0.2.6-binary-assembly-hdfs-1.0.4.zip) |
| For HDFS cdh-4.1 users | [distribution zip](https://hyracks.googlecode.com/files/pregelix-dist-0.2.6-binary-assembly-hdfs-cdh-4.1.zip)  |
| For HDFS cdh-4.2 users |[distribution zip](https://hyracks.googlecode.com/files/pregelix-dist-0.2.6-binary-assembly-hdfs-cdh-4.2.zip)  |

**Note1**: if your HDFS has a **compatible client** to any of those versions, things will still work.

**Note2**: for **CDH** users, you need to manually add the following into your core-site.xml (/etc/hadoop/conf/core-site.xml):
```
<property>
  <name>fs.hdfs.impl</name>
  <value>org.apache.hadoop.hdfs.DistributedFileSystem</value>
</property>
```

After you download the zip file, copy it to your desired directory and unzip it.  Enter the unzipped directory and do a "ls", you will see:
```
bin	 conf		data		examples	lib
```

We call the unzipped directory **distribution directory** in the following part of the document.  **When you use pregelix, you need to go to that directory on the master machine first**.

## Build from the source ##
Download the source code:
```
git clone https://code.google.com/p/hyracks
```

Build the whole Hyracks/Pregelix software stack (**shutdown all active Hyracks/Pregelix instances during the build**):
```
cd hyracks
git checkout fullstack-0.2.6
mvn clean package -DskipTests=true
```

For different HDFS versions, the build command is different
| HDFS-0.20.2 | mvn clean package -DskipTests=true |
|:------------|:-----------------------------------|
| HDFS-0.23.1 | mvn clean package -DskipTests=true -Dhadoop=0.23.1 |
| HDFS-0.23.6 | mvn clean package -DskipTests=true -Dhadoop=0.23.6 |
| HDFS-1.0.4 | mvn clean package -DskipTests=true -Dhadoop=1.0.4 |
| HDFS-cdh-4.1 | mvn clean package -DskipTests=true -Dhadoop=cdh-4.1 |
| HDFS-cdh-4.2 | mvn clean package -DskipTests=true -Dhadoop=cdh-4.2 |

After build, you should see "BUILD SUCCESS" on your console.
Then, the follow directory:
```
pregelix/pregelix-dist/target/pregelix-dist-0.2.6-binary-assembly
```
is equivalent to the unzipped downloaded .zip file:-)

# Running Pregelix locally #
As a test, we can try a local Pregelix cluster first. The default configurations work for a local cluster with one master process and one slave process.

## Configure password-free ssh ##
Try to ssh to localhost first:
```
ssh localhost
```
If the console asks for the password, try the following:
```
cd ~/.ssh
ssh-keygen -t rsa  #just press "enter" when you are asked for passphrase
more id_rsa.pub >> authorized_keys
chmod 600 authorized_keys
```

## Start the local Pregelix cluster ##
Install ssh sever if your machine does not have that, for example, open-ssh. [Here](http://www.cyberciti.biz/faq/ubuntu-linux-openssh-server-installation-and-configuration/) illustrates how to install open-ssh. Then, **make sure the ssh server is started** on your machine.

Enter the distribution directory.
Set JAVA\_HOME in conf/cluster.properties to the path of your desired JDK, for example,  /usr/java/jdk-1.6. Start cluster by:
```
bin/startCluster.sh
```

Verify that the cluster is indeed started:
```
jps|grep CCDriver #find and list the master process
jps|grep NCDriver #find and list the slave process
```
The cluster is indeed started only when the master process and the slave process can both be found. Or you can open **http://<master node DNS name>:16001/adminconsole** to see if all slaves are listed there.

If either the master process or the slave process is not started, check the reasons by looking at the logs:
```
cat /tmp/t1/logs/*.log  #check the master process log
cat /tmp/t2/logs/*.log  #check the slave process log
```

Export HADOOP\_HOME to a non-existent path so that Pregelix can automatically use the local file system instead of HDFS:
```
export HADOOP_HOME=.
```

## Run example jobs locally ##
1. View the test graph (in the form of adjacency list):
```
cat data/webmap/*
```
The test graph has the following schema:
|source vertex identifer|space separated outgoing neighbors|
|:----------------------|:---------------------------------|

2. Run the PageRank example locally and view the results:
```
bin/pregelix examples/pregelix-example-0.2.6-jar-with-dependencies.jar edu.uci.ics.pregelix.example.PageRankVertex -inputpaths data/webmap -outputpath /tmp/pg_result -ip `bin/getip.sh` -port 3099 -vnum 20 -num-iteration 5

cat /tmp/pg_result/part*
```
The output result has the following schema:
|vertex identifer|rank value|
|:---------------|:---------|

3. Run the connected components example locally and view the results:
```
bin/pregelix examples/pregelix-example-0.2.6-jar-with-dependencies.jar edu.uci.ics.pregelix.example.ConnectedComponentsVertex -inputpaths data/webmap -outputpath /tmp/cc_result -ip `bin/getip.sh` -port 3099

cat /tmp/cc_result/part*
```
The output result has the following schema:
|vertex identifer|component identifier (the minimum vertex identifer in the component)|
|:---------------|:-------------------------------------------------------------------|

4. Run the shortest paths example locally (calculate the shortest path distances from vertex 10 to every vertex in the graph) and view the results:
```
bin/pregelix examples/pregelix-example-0.2.6-jar-with-dependencies.jar edu.uci.ics.pregelix.example.ShortestPathsVertex -inputpaths data/webmap -outputpath /tmp/sp_result -ip `bin/getip.sh` -port 3099 -source-vertex 10

cat /tmp/sp_result/part*
```
The output result has the following schema:

|vertex identifer|distance to the source vertex|
|:---------------|:----------------------------|

## Stop the local Pregelix cluster ##
```
bin/stopCluster.sh
```

# Running Pregelix distributedly #
Pregelix can run on a parallel share-nothing cluster, where there is one master machine and a large number of slave machines.

## Download ##
If there is a [NFS](http://en.wikipedia.org/wiki/Network_File_System) installed in the cluster, then we ONLY need to download one copy of the Pregelix to a NFS directory of the master machine, because every machine can access the NFS directory; otherwise, we have to download the Pregelix distribution zip to each machine and unzip it with **the same directory path**.

Unzip the downloaded Pregelix zip file to your desired **distribution directory**.  **When you use pregelix, you need to go to the distribution directory first**.

To quickly determine whether there is a NFS installed, run the df command:
```
$df
Filesystem           1K-blocks      Used Available Use% Mounted on
                      67446384  51136956  12883332  80% /
udev                   2064396       264   2064132   1% /dev
none                  67446384  51136956  12883332  80% /var/lib/ureadahead/debugfs
/dev/sdb5               233335     66771    154116  31% /boot
*128.135.22.177:/home 309637120 284829856   9078624  97% /home*
```
If the /home directory is mounted to a remote location as above, that means the NFS is installed.

Download Pregelix:
| For default HDFS (0.20.2) users | [distribution zip](https://hyracks.googlecode.com/files/pregelix-dist-0.2.6-binary-assembly-hdfs-0.20.2.zip) |
|:--------------------------------|:-------------------------------------------------------------------------------------------------------------|
| For HDFS-0.23.1 users | [distribution zip](https://hyracks.googlecode.com/files/pregelix-dist-0.2.6-binary-assembly-hdfs-0.23.1.zip)  |
| For HDFS-0.23.6 users | [distribution zip](https://hyracks.googlecode.com/files/pregelix-dist-0.2.6-binary-assembly-hdfs-0.23.6.zip)  |
| For HDFS-1.0.4 users | [distribution zip](https://hyracks.googlecode.com/files/pregelix-dist-0.2.6-binary-assembly-hdfs-1.0.4.zip) |
| For HDFS cdh-4.1 users | [distribution zip](https://hyracks.googlecode.com/files/pregelix-dist-0.2.6-binary-assembly-hdfs-cdh-4.1.zip)  |
| For HDFS cdh-4.2 users |[distribution zip](https://hyracks.googlecode.com/files/pregelix-dist-0.2.6-binary-assembly-hdfs-cdh-4.2.zip)  |

## Configure password-free ssh for the cluster ##
Try to ssh to localhost first:
```
ssh localhost
```
If the console asks for the password, try the following:
```
cd ~/.ssh
ssh-keygen -t rsa #just press "enter" when you are asked for passphrase
more id_rsa.pub >> authorized_keys
chmod 600 authorized_keys
```

If there is no NSF installed, create the same account on every machine (if there is not such an account), copy the id\_rsa.pub to the directory ~/.ssh (login with the same account) on each machine, and then do the following on each machine:
```
cd ~/.ssh
more id_rsa.pub >> authorized_keys
chmod 600 authorized_keys
```

## Configure master/slaves ##
Enter the Pregelix distribution on the master machine.
Put the DNS name of master node into conf/master, for example:
```
master_node
```

Put the DNS names of slave nodes into conf/slaves(one-per-line), for example:
```
slave_node1
slave_node2
.....
```

## Set up cluster configurations ##
We only need to configure a few properties on the master machine.

1. Walk through the conf/cluster.properties, and set each property according to your cluster environment. The following is just a template, please **customize it according to your system**.
```
#The CC (the master) port for Hyracks clients
CC_CLIENTPORT=3099

#The CC (the master) port for Hyracks cluster management
CC_CLUSTERPORT=1099

#The tmp directory for cc (the master) to install jars
CCTMP_DIR=/tmp/t1

#The tmp directory for nc (the slave) to install jars
NCTMP_DIR=/tmp/t2

#The directory to put cc (the master) logs
CCLOGS_DIR=$CCTMP_DIR/logs

#The directory to put nc (the slave) logs
NCLOGS_DIR=$NCTMP_DIR/logs

#Comma separated I/O directories for the spilling of external sort
#Usually, there is one directory on each disk drive to get disk stripping
IO_DIRS="/tmp/t3,/tmp/t4"

#The JAVA_HOME (on all master and slaves) to run all binaries
JAVA_HOME=$JAVA_HOME

#The frame size of the internal dataflow engine
FRAME_SIZE=65536

#CC (master node) JAVA_OPTS
CCJAVA_OPTS="-Xmx3g"

#NC (slave node) JAVA_OPTS
NCJAVA_OPTS="-Xmx3g"

```

Here is the summary of the properites:
|property name|meaning|
|:------------|:------|
|CC\_CLIENTPORT|The port for Hyracks master demon to accept new client connections|
|CC\_CLUSTERPORT|The port for Hyracks master demon to manage the cluster, e.g, talking to slave nodes|
|CCTMP\_DIR|The temp directory of the master node, used to install jars.|
|NCTMP\_DIR|The temp directory of the slave nodes, used to install jars.|
|CCLOGS\_DIR|The log directory of the master log.|
|NCLOGS\_DIR|The log directory of the slave log.|
|IO\_DIRS|Comma separated I/O directories for spilling the intermedaite data. Usually we use **one directory per disk drive** to get disk stripping.|
|JAVA\_HOME|The JAVA\_HOME directory on each machine|
|FRAME\_SIZE|The frame (page) size in the pregelix jobs. Any vertex/message should be able to fit in a page.|
|CCJAVA\_OPTS|The JAVA\_OPTS (JVM parameters) for the master node.|
|NCJAVA\_OPTS|The JAVA\_OPTS (JVM parameters) for the slave nodes.|

**Important**: Never use a NFS directory for any directory property, because the remote disk access will slow down the overall performance.

2. Configure the graph store locations on a slave node, in conf/stores.properties. Here is the template:
```
#Comma separated directories for storing the partitioned graph on each machine
store=/tmp/teststore1,/tmp/teststore2
```
|property name|meaning|
|:------------|:------|
|store|Comma separated directories for storing the partitioned graph on each machine.  Usually we use **one directory per disk drive** to get disk stripping.|

## Set up HDFS cluster ##
1. Create a HDFS cluster using a desired Hadoop version which we support or which is client-compatible with the ones we support. You can install **Hadoop-0.20.2**, according to a [quick tutorial](http://code.google.com/p/hyracks/wiki/ConfigHDFS). If you want to know more tricks for HDFS performance tuning, you can read [this book](http://www.amazon.com/Hadoop-Definitive-Guide-Tom-White/dp/0596521979).

2. In the current shell, export the right HADOOP\_HOME (this is very important, because Pregelix relies on HADOOP\_HOME to find the HDFS to grab data and write results), for example:
```
export HADOOP_HOME=~/hadoop-0.20.2
```

3. Load your own graph dataset to the Hadoop cluster, for example:
```
$HADOOP_HOME/bin/hadoop dfs -mkdir /webmap
$HADOOP_HOME/bin/hadoop dfs -put data/webmap/* /webmap/
```

## Run example jobs distributedly ##
1. Start the Pregelix cluster
```
bin/startCluster.sh
```
You can open **http://<master node DNS name>:16001/adminconsole** to verify if all slaves are listed there. If some slaves are missing, please ssh to those machines and read the slave logs in the slave log directory (set in conf/cluster.properties) to see what is going on.

2. Run the three examples in the same way as [running them on locally](http://code.google.com/p/hyracks/wiki/PregelixUserManual#Run_example_jobs_locally). The difference is that the input/output file paths are HDFS paths now.

Let us take PageRank as an example:

View the test graph (in the form of adjacency list) (skip that if the graph is too large):
```
$HADOOP_HOME/bin/hadoop dfs -cat /webmap/*
```
The test graph should have the following schema:
|source vertex identifer|space separated outgoing neighbors|
|:----------------------|:---------------------------------|

Run the PageRank example distributedly and view the results (skip viewing results if the graph is too large):
```
bin/pregelix examples/pregelix-example-0.2.6-jar-with-dependencies.jar edu.uci.ics.pregelix.example.PageRankVertex -inputpaths /webmap -outputpath /tmp/pg_result -ip `bin/getip.sh` -port 3099 -vnum 20 -num-iteration 5

$HADOOP_HOME/bin/hadoop dfs -cat /tmp/pg_result/part*
```
The output result has the following schema:
|vertex identifer|rank value|
|:---------------|:---------|


3. Stop the Pregelix cluster
```
bin/stopCluster.sh
```

# Performance tuning #
## Plan choice ##
In Pregelix, there are mainly two different execution strategies for a job. They both lead to the correct result, but may have different performance for the same Pregelix job implementation.  We summarize their properties here:

|Plan choice|Suitable scenarios|Algorithm examples|
|:----------|:-----------------|:-----------------|
|OUTER\_JOIN|Every vertex is alive or receives messages in an iteration.|PageRank|
|INNER\_JOIN|Only a portion of vertices are alive or receive messages in an iteration|ShortestPaths, ConnectedComponents, Reachibility|

To run our existing [examples](http://code.google.com/p/hyracks/wiki/PregelixUserManual#Run_example_jobs_locally) with specified plan choice, one can add either "-plan OUTER\_JOIN" or "-plan INNER\_JOIN" to the execution command lines.

## Performance monitoring ##
You can monitor various performance statistics (CPU usage, memory usage, network traffic, JVM garbage collection, thread statistics, and IPC statistics) on each slave node through the Hyracks adminconsole during job excutions, at http://<master node>:16001/adminconsole.

## Performance debugging ##
We can use a Java profiler to investigate performance problems in your application implementations.  Here is an example that uses the [Yourkit](http://www.yourkit.com) profiler.
To use Yourkit, you can **append the Yourkit agent specification** to JAVA\_OPTS.

For example, you can set the CCJAVA\_OPTS property in conf/cluster.properties as follows to bind yourkit agent to the JVM running the master process.
```
CCJAVA_OPTS="Xmx3g -agentpath:/grid/0/dev/yingyib/tools/yjp-10.0.4/bin/linux-x86-64/libyjpagent.so=port=20001"
```

For example, you can also set the NCJAVA\_OPTS property in conf/cluster.properties to bind yourkit agent to all the JVM running the slave processes.
```
NCJAVA_OPTS="Xmx3g -agentpath:/grid/0/dev/yingyib/tools/yjp-10.0.4/bin/linux-x86-64/libyjpagent.so=port=20001"
```

Please **customize the agentpath** to your own setting.
Then, you can connect to the JVM (either master or slave) from a Yourkit GUI on your desktop machine.

## JVM tuning ##
Here are several important JVM parameters (in either CCJAVA\_OPTS or NCJAVA\_OPTS, in conf/properties) for performance tuning:
|Parameter|Usage|Example|Guideline|
|:--------|:----|:------|:--------|
|-Xmx|The maximum allowed heap usage.|-Xmx3g|2/3 of the physical memory.|
|-XX:MaxPermSize|The maximum allowed permenant generation size.|-XX:MaxPermSize=2g|Usually we do not set that and the default value works fine.|


# Troubleshooting #
In this section, we first walk through local machine debugging, and then distributed debugging. Since the application code is only running in slave processes, we only need to change slave JAVA\_OPTS to debug.

## Setting debug parameters ##
If you want to debug your problem on the local cluster, append the following line into your NCJAVA\_OPTS (in conf/cluster.properties):
```
-Xdebug -Xrunjdwp:transport=dt_socket,address=7001,server=y,suspend=n
```

Restart the local cluster:
```
bin/stopCluster.sh
bin/startCluster.sh
```

Then, you can debug with breakpoints in your Eclipse, using remote debugging, for example, connect your Eclipse to the slave process at port 7001.


## Setting yet-another local slave process to debug ##
Sometimes, one debugging slave process may not be enough and you may want to have two local slave processes to better simulate the distributed scenarios.  In that case, you can start yet-another local slave process for the debugging purpose.

First, edit the conf/debugnc.properties:
```
#The tmp directory for nc to install jars
NCTMP_DIR2=/tmp/t-1

#The directory to put nc logs
NCLOGS_DIR2=$NCTMP_DIR/logs

#Comma separated I/O directories for spilling data
IO_DIRS2="/tmp/t-2,/tmp/t-3"

#NC JAVA_OPTS
NCJAVA_OPTS2="-Xdebug -Xrunjdwp:transport=dt_socket,address=7003,server=y,suspend=n -Xmx1g -Djava.util.logging.config.file=logging.properties"
```
Note that above setting is just a **template**. Please change property values **according to your system setting**.  The most important thing in conf/debugnc.properties is that you have to use yet-another debugging port for this new slave.

start the new debugging slave process:
```
bin/startDebugNc.sh
```
Now you can connect your Eclipse to the new slave process at port 7003.


## Debugging distributed cluster ##
We assume you already have a distributed cluster running.
Append the following line into your NCJAVA\_OPTS (in conf/cluster.properties):
```
-Xdebug -Xrunjdwp:transport=dt_socket,address=7003,server=y,suspend=n
```

Restart the distributed cluster:
```
cd pregelix/pregelix-dist/target/appassembler
bin/stopCluster.sh
bin/startCluster.sh
```

Then, you can debug with breakpoints in your Eclipse, using remote debugging, for example, connect your Eclipse to any remote slave process at port 7003.


# Getting help #
If you encounter a problem, please send an email to pregelix-users@googlegroups.com

# Report bugs #
If you find a bug, please send an email to pregelix-users@googlegroups.com, or open an issue in http://code.google.com/p/hyracks/issues/list.