
---


## Introduction ##
IMRU is an open-source implementation of [Iterative Map Reduce Update](http://arxiv.org/abs/1203.0160).

IMRU provides a general framework for parallelizing the class of machine-learning algorithms that correspond to the statistical query model. For instance, this class includes batch learning and [Expectation Maximization](http://en.wikipedia.org/wiki/Expectation%E2%80%93maximization_algorithm).
A machine learning algorithm in this class can be split into three steps:

  * A **map** step, where all training data is evaluated against the current model.

  * A **reduce** step, where the outputs of the map step are aggregated. The aggregation function is assumed to be commutative and associative.

  * A **update** step, where the model is revised based on the result of the reduce step.

This sequence repeats iteratively until the model converges. The following figure shows how data flows inside IMRU.

![http://hyracks.googlecode.com/svn/wiki/images/imru/imruLogical.png](http://hyracks.googlecode.com/svn/wiki/images/imru/imruLogical.png)

To instantiate an algorithm in the IMRU programming framework, a programmer has to provide three functions that correspond to the previous steps. IMRU takes care of parallelizing the evaluation of each step and moving data between steps in an efficient manner. The programmer must also provide a method **shouldTerminate**, which is called at the end of each iteration in order to determine whether the model has converged. Typical conditions for convergence may be that the number of iterations has exceeded a threshold, or that the model has not changed much from the previous iteration.

## Build IMRU ##

### Prerequisite ###
Supported platforms:
Linux (kernel 2.6.x or above) or Mac (OS X 10.5 or above), either 32bit or 64bit.

Software requirement:

|svn (the Subversion version control system)|
|:------------------------------------------|
|maven (http://maven.apache.org/), note that we need to use maven version **3.x.x**.|
|Java 1.7.x or above|

### Getting IMRU ###
1. Download IMRU:
```
$cd ~/
$git clone https://code.google.com/p/hyracks/
$cd hyracks
$git pull origin rui/imru_full_stack:rui/imru_full_stack
$git checkout rui/imru_full_stack
```

2. Build IMRU:
```
cd ~/hyracks
export MAVEN_OPTS=-Xmx512m
mvn package -DskipTests=true
chmod -R 755 imru/imru-dist/target/appassembler/bin/*
```
Proceed only when you see "BUILD SUCCESSFUL" on your screen after "mvn package -DskipTests=true". The build needs to download files over the network. It may take several minutes to an hour depending on your network speed. Note that the "chmod -R 755 ." command is to make all the binaries executable.

### Import IMRU examples to eclipse ###
1. Move generated eclispse project out. For example
```
mv ~/fullstack_imru/imru/imru-example/target/eclipse ~/workspace/imru-examples
```
**Note**: This is for classical eclipse. If you are using maven eclipse, please import <home directory>/fullstack\_imru/imru/imru-example directly.

2. Open eclipse, from the menu, select File->Import

![http://wiki.hyracks.googlecode.com/git/images/imru/eclipse1.png](http://wiki.hyracks.googlecode.com/git/images/imru/eclipse1.png)

3. Select General->Existing Projects into Workspace

![http://wiki.hyracks.googlecode.com/git/images/imru/eclipse2.png](http://wiki.hyracks.googlecode.com/git/images/imru/eclipse2.png)

4. In the **select root directory** box, input <home directory>/workspace/imru-examples. Then press **Enter**. Click Finish

![http://wiki.hyracks.googlecode.com/git/images/imru/eclipse3.png](http://wiki.hyracks.googlecode.com/git/images/imru/eclipse3.png)

5. In eclipse menu, select Window->Preferences

![http://wiki.hyracks.googlecode.com/git/images/imru/eclipse_add_jdk7_1.png](http://wiki.hyracks.googlecode.com/git/images/imru/eclipse_add_jdk7_1.png)

In Java->Installed JREs, click Add

![http://wiki.hyracks.googlecode.com/git/images/imru/eclipse_add_jdk7_2.png](http://wiki.hyracks.googlecode.com/git/images/imru/eclipse_add_jdk7_2.png)

Input JDK 1.7 path and click Finish

![http://wiki.hyracks.googlecode.com/git/images/imru/eclipse_add_jdk7_3.png](http://wiki.hyracks.googlecode.com/git/images/imru/eclipse_add_jdk7_3.png)

Select JDK 1.6 and click Remove

![http://wiki.hyracks.googlecode.com/git/images/imru/eclipse_add_jdk7_4.png](http://wiki.hyracks.googlecode.com/git/images/imru/eclipse_add_jdk7_4.png)

Then press OK

## Test IMRU in one process ##

#### Run IMRU in eclipse ####
For developing and debugging, we can run everything in one process. `KMeans.java` is an example.

In eclipse imru-example project, open and run [src/main/java/edu/uci/ics/hyracks/imru/example/kmeans/KMeans.java](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/imru/imru-example/src/main/java/edu/uci/ics/hyracks/imru/example/kmeans/KMeans.java)

**Note**: If there isn't a local cluster already, `KMeans.java` will

1. setup a local cluster controller running in the same process.

2. setup two local node controllers named "nc1" and "nc2" running in the same process.

3. generate a HAR will all code and upload it to the cluster

4. run the job and print the result

#### Run IMRU in cmdline ####

Enter the distribution directory:
```
cd ~/fullstack_imru/imru/imru-dist/target/appassembler/
```

Run the K-Means example:
```
bin/imru examples/imru-example-0.2.3-SNAPSHOT.jar edu.uci.ics.hyracks.imru.example.kmeans.KMeans -debug -disable-logging -host localhost -port 3099 -app kmeans -example-paths ../../../imru-example/data/kmeans/kmeans0.txt,../../../imru-example/data/kmeans/kmeans1.txt
```

**Note**: The `-debug` option will start a cluster controller, two node controller and submit the job in one process.

## Developing your own code ##
The helloworld example in [src/main/java/edu/uci/ics/hyracks/imru/example/helloworld](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/imru/imru-example/src/main/java/edu/uci/ics/hyracks/imru/example/helloworld) package is a simple template to develop your own IMRU job.
Modify the parse(), map(), reduce(), update() and other functions in [HelloWorldJob.java](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/imru/imru-example/src/main/java/edu/uci/ics/hyracks/imru/example/helloworld/HelloWorldJob.java).

Here is a summary of these functions:
| function | description |
|:---------|:------------|
| parse() | The input is an input stream of one input file. The output is data objects. |
| map() | The input is an iterator of data object. The output is one map result. |
| reduce() | The input is an iterator of map result. The output is one reduce result |
| update() | The input is an iterator of reduce result. This function should update the model.  |

Modify [HelloWorldModel.java](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/imru/imru-example/src/main/java/edu/uci/ics/hyracks/imru/example/helloworld/HelloWorldModel.java) to store the model which is used in map() and updated in update().

Modify [HelloWorldData.java](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/imru/imru-example/src/main/java/edu/uci/ics/hyracks/imru/example/helloworld/HelloWorldData.java) to store data produced by parse() and used by map().

Modify [HelloWorldResult.java](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/imru/imru-example/src/main/java/edu/uci/ics/hyracks/imru/example/helloworld/HelloWorldResult.java) to store result produced by map() and reduce().

Refactor package and file names as needed. More details can be found in [IMRU Development Manual](http://code.google.com/p/hyracks/wiki/ImruDevManual).

## Running IMRU in a cluster ##

#### Configure password-free ssh ####
Install ssh sever if your machine does not have that, for example, open-ssh. [This page](http://www.cyberciti.biz/faq/ubuntu-linux-openssh-server-installation-and-configuration/) illustrates how to install open-ssh. Then, **make sure the ssh server is started** on your machine.

Try to ssh to localhost first:
```
ssh localhost
```
If the console asks for the password, try the following:
```
cd ~/.ssh
ssh-keygen -t rsa  #just press "enter" when you are asked for passphrase
cat id_rsa.pub >> authorized_keys
chmod 600 authorized_keys
```

### Running IMRU in a one machine cluster ###
To test our code in a distributed environment, we can first setup a local IMRU cluster. The following guide creates a local cluster with one master process and one slave process.

#### Start the local Hyracks cluster ####
Enter the distribution directory:
```
cd ~/fullstack_imru/imru/imru-dist/target/appassembler/
```
Set JAVA\_HOME in conf/cluster.properties to the path of your desired JDK, for example,  /usr/java/jdk1.7.0\_10. Start cluster by:
```
bin/startCluster.sh
```

Verify that the cluster is indeed started:
```
ps -ef|grep hyrackscc #find and list the master process
ps -ef|grep hyracksnc #find and list the slave process
```
The cluster is indeed started only when the master process and the slave process can both be found. Or you can open **http://<master node DNS name>:16001/adminconsole** to see if all slaves are listed there.

If either the master process or the slave process is not started, check the reasons by looking at the logs:
```
cat /tmp/t1/logs/*.log  #check the master process log
cat /tmp/t2/logs/*.log  #check the slave process log
```

#### Run example jobs locally ####

#### Run IMRU in eclipse ####
In eclipse imru-example project, open and run [src/main/java/edu/uci/ics/hyracks/imru/example/kmeans/KMeans.java](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/imru/imru-example/src/main/java/edu/uci/ics/hyracks/imru/example/kmeans/KMeans.java)

**Note**: `KMeans.java` will

1. connect to the local cluster controller

2. generate a HAR with all code and upload it to the cluster

3. run the job and print the result

If anything went wrong, check node controller log
```
cat /tmp/t2/logs/*.log  #check the slave process log
```

#### Run IMRU in cmdline ####

Run the K-Means example:
```
bin/imru examples/imru-example-0.2.3-SNAPSHOT.jar edu.uci.ics.hyracks.imru.example.kmeans.KMeans -host `bin/getip.sh` -port 3099 -app kmeans -example-paths `cd ~; pwd`/fullstack_imru/imru/imru-example/data/kmeans/kmeans0.txt,`cd ~; pwd`/fullstack_imru/imru/imru-example/data/kmeans/kmeans1.txt -agg-tree-type generic -agg-count 1
```

#### Stop the local Hyracks cluster ####
```
bin/stopCluster.sh
```

### Running IMRU in a multi-machine cluster ###
IMRU can run on a parallel share-nothing cluster, where there is one master machine and a large number of slave machines.

#### Download and build ####
Download IMRU:

Login to the master node using ssh, and then:
```
cd ~/
$svn co https://hyracks.googlecode.com/svn/branches/fullstack_imru fullstack_imru
```

Build IMRU:
```
cd fullstack_imru
export MAVEN_OPTS=-Xmx512m
mvn package -DskipTests=true
chmod -R 755 .
```

#### Configure master/slaves ####
Enter the build target directory of IMRU on the master machine:
```
cd ~/fullstack_imru/imru/imru-dist/target/appassembler
```

Put the DNS name of master node into conf/master, for example:
```
master_node
```

Put the DNS names or IP address of slave nodes into conf/slaves(one-per-line), for example:
```
slave_node1
slave_node2
.....
```

#### Set up cluster configurations ####
We only need to configure a few properties on the master machine.

Walk through the conf/cluster.properties, and set each property according to your cluster environment. The following template can be used as a default template without any modification.
```
#The CC (the master) port for Hyracks clients
CC_CLIENTPORT=3099

#The CC (the master) port for Hyracks cluster management
CC_CLUSTERPORT=1099

#The directory of the local Hyracks codebase
HYRACKS_HOME=../../../../hyracks

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
|CC\_CLIENTPORT|The port for hyracks master demon to accept new client connections|
|CC\_CLUSTERPORT|The port for hyracks master demon to manage the cluster, e.g, talking to slave nodes|
|HYRACKS\_HOME|The directory for downloaded hyracks project, at the same directory level of the IMRU project|
|CCTMP\_DIR|The temp directory of the master node, used to install jars.|
|NCTMP\_DIR|The temp directory of the slave nodes, used to install jars.|
|CCLOGS\_DIR|The log directory of the master log.|
|NCLOGS\_DIR|The log directory of the slave log.|
|IO\_DIRS|Comma separated I/O directories for spilling the intermedaite data. Usually we use **one directory per disk drive** to get disk stripping.|
|JAVA\_HOME|The JAVA\_HOME directory on each machine|
|FRAME\_SIZE|The frame (page) size in the IMRU jobs. Any tuple should be able to fit in a page.|
|CCJAVA\_OPTS|The JAVA\_OPTS (JVM parameters) for the master node.|
|NCJAVA\_OPTS|The JAVA\_OPTS (JVM parameters) for the slave nodes.|

#### Deploy hyracks to the cluster ####
Run the following command to deploy hyracks to all slave nodes.

```
~/fullstack_imru/imru/imru-dist/target/appassembler/bin/rsyncAllNCs.sh
```

This command will

1. generate `<home directory>/.ssh/id_rsa.pub` if it's not exist.

2. Add `<home directory>/.ssh/id_rsa.pub` on master node to `<home directory>/.ssh/authorized_keys` on each slave node.

3. Synchronize `/fullstack_imru/imru/imru-dist/target/appassembler` to all slave nodes.

**Note:** During the first time, you may need to input login password for each slave node if you haven't already enabled public key login. After that, master node has access to slave nodes without using password.

#### Run example jobs distributedly ####
1. Start the Hyracks cluster
```
bin/startCluster.sh
```
You can open **http://<master node DNS name>:16001/adminconsole** to verify if all slaves are listed there. If some slaves are missing, please ssh to those machines and read the slave logs in the slave log directory (set in conf/cluster.properties) to see what is going on.

2. Run the kmeans examples in the same way as [running them locally](http://code.google.com/p/hyracks/wiki/ImruUserManual#Run_example_jobs_locally).

3. Stop the Hyracks cluster
```
bin/stopCluster.sh
```


### Troubleshooting ###
In this section, we first walk through local machine debugging, and then distributed debugging. Since the application code is only running in slave processes, we only need to change slave JAVA\_OPTS to debug.

#### Setting debug parameters ####
If you want to debug your problem on the local cluster, append the following line into your NCJAVA\_OPTS (in conf/cluster.properties):
```
-Xdebug -Xrunjdwp:transport=dt_socket,address=7001,server=y,suspend=n
```

Restart the local cluster:
```
cd imru/imru-dist/target/appassembler
bin/stopCluster.sh
bin/startCluster.sh
```

Then, you can debug with breakpoints in your Eclipse, using remote debugging, for example, connect your Eclipse to the slave process at port 7001.


#### Setting yet-another local slave process to debug ####
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


#### Debugging distributed cluster ####
We assume you already have a distributed cluster running.
Append the following line into your NCJAVA\_OPTS (in conf/cluster.properties):
```
-Xdebug -Xrunjdwp:transport=dt_socket,address=7001,server=y,suspend=n
```

Restart the distributed cluster:
```
cd imru/imru-dist/target/appassembler
bin/stopCluster.sh
bin/startCluster.sh
```

Then, you can debug with breakpoints in your Eclipse, using remote debugging, for example, connect your Eclipse to any remote slave process at port 7001.

### Getting help ###
If you encounter a problem, please send an email to imru-users@googlegroups.com

### Report bugs ###
If you find a bug, please send an email to imru-users@googlegroups.com, or open an issue in http://code.google.com/p/hyracks/issues/list.

### Reference ###

`[1]` [Scaling Datalog for Machine Learning on Big Data](http://arxiv.org/abs/1203.0160)