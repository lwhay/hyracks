


---


# Introduction #
Hivesterix is an open-source implementation of [HiveQL](https://cwiki.apache.org/confluence/display/Hive/Home), which runs on the Hyracks runtime instead of Hadoop.  Hivesterix-0.2.8 uses Hive-0.11.0 as the libraries.

# Prerequisite #
Supported platforms:
Linux (kernel 2.6.x or above) or Mac (OS X 10.7 or above), either 32bit or 64bit.

Software requirement:
|  **Java 1.7.x or above** |
|:-------------------------|

# Getting Hivesterix #
## Download Hivesterix ##
| For default HDFS (0.20.2) users | [distribution zip](http://isg.ics.uci.edu/release/hivesterix/0.2.8/dist/hivesterix-dist-binary-assembley-hdfs-0.20.2.zip) |
|:--------------------------------|:--------------------------------------------------------------------------------------------------------------------------|
| For HDFS-0.23.1 users | [distribution zip](http://isg.ics.uci.edu/release/hivesterix/0.2.8/dist/hivesterix-dist-binary-assembley-hdfs-0.23.1.zip)  |
| For HDFS-0.23.6 users | [distribution zip](http://isg.ics.uci.edu/release/hivesterix/0.2.8/dist/hivesterix-dist-binary-assembley-hdfs-0.23.6.zip)  |
| For HDFS-1.0.4 users | [distribution zip](http://isg.ics.uci.edu/release/hivesterix/0.2.8/dist/hivesterix-dist-binary-assembley-hdfs-1.0.4.zip) |
| For HDFS cdh-4.1 users | [distribution zip](http://isg.ics.uci.edu/release/hivesterix/0.2.8/dist/hivesterix-dist-binary-assembley-hdfs-cdh-4.1.zip)  |
| For HDFS cdh-4.2 users |[distribution zip](http://isg.ics.uci.edu/release/hivesterix/0.2.8/dist/hivesterix-dist-binary-assembley-hdfs-cdh-4.2.zip)  |

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
bin    conf    lib
```

We call the unzipped directory **distribution directory** in the following part of the document.  **When you use Hivesterix, you need to go to that directory on the master machine first**.

## Build from the source ##
Download the source code:
```
git clone https://code.google.com/p/hyracks
```

Build the whole Hyracks/Hivesterix software stack (**shutdown all active Hyracks/Hivesterix instances during the build**):
```
cd hyracks
git checkout fullstack-0.2.8
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
Then, the following directory:
```
hivesterix/hivesterix-dist/target/hivesterix-dist-0.2.8-binary-assembly
```
is equivalent to the unzipped downloaded .zip file:-)

# Running Hivesterix #
Hivesterix can run on a parallel share-nothing cluster, where there is one master machine and a large number of slave machines.

## The directory for binaries ##
If there is a [NFS](http://en.wikipedia.org/wiki/Network_File_System) installed in the cluster, then we ONLY need to download or build one copy of the Hivesterix to a NFS directory of the master machine, because every machine can access the NFS directory; otherwise, we have to download or build Hivesterix on each machine, with **the same directory path**.

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
Enter the Hivesterix distribution on the master machine.
Put the **DNS name** of master node into conf/master, for example:
```
master_node
```

Put the **DNS names** of slave nodes into conf/slaves(one-per-line), for example:
```
slave_node1
slave_node2
.....
```

## Set up cluster configurations ##
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

Make sure the conf directories on all the slave machines have the same content as the master machine.

## Set up HDFS cluster ##
1. Create a HDFS cluster using a desired Hadoop version which we support or which is client-compatible with the ones we support. For example, you can install Hadoop-0.20.2, according to a [quick tutorial](http://code.google.com/p/hyracks/wiki/ConfigHDFS). If you want to know more tricks for HDFS performance tuning, you can read [this book](http://www.amazon.com/Hadoop-Definitive-Guide-Tom-White/dp/0596521979).

2. In the current shell, export the right HADOOP\_HOME (this is very important, because Hivesterix relies on HADOOP\_HOME to find the HDFS to grab data and write results), for example:
```
export HADOOP_HOME=~/hadoop-0.20.2
```

3. Load your own test data to the Hadoop cluster, for example:
```
$HADOOP_HOME/bin/hadoop dfs -mkdir /tpch
$HADOOP_HOME/bin/hadoop dfs -put /<local data path> /tpch
```

## Run queries ##
1. Enter the Hivesterix distribution directory and start the Hivesterix cluster
```
bin/startCluster.sh
```
You can open **http://<master node DNS name>:16001/adminconsole** to verify if all slaves are listed there. If some slaves are missing, please ssh to those machines and read the slave logs in the slave log directory (set in conf/cluster.properties) to see what is going on.

2. Using Hivesterix is the same as using Hive:
```
bin/hive
```
It supports the same command line options as Hive!
You can read this tutorial if you are not familiar with Hive:
https://cwiki.apache.org/confluence/display/Hive/LanguageManual

3. Stop the Hyracks cluster
```
bin/stopCluster.sh
```

# Performance tuning #
## Configuration tuning ##
Similar to Hive, there is a hive-default.xml under
hivesterix/hivesterix-dist/target/appassembler/conf.  In addition to Hive's parameters, there are four additional parameters you can tune:
```
         <!-- number of partitions per machine -->
        <property>
                <name>hive.hyracks.parrallelism</name>
                <value>4</value>
        </property>

        <!--True: using the external hash group-by; False: using the sort-based group-by-->
        <property>
                <name>hive.algebricks.groupby.external</name>
                <value>true</value>
        </property>
       
        <!--The buffer size in bytes for external group-by.  The following is the recommended configuration.-->
        <property>
                <name>hive.algebricks.groupby.external.memory</name>
                <value>33554432</value>
        </property>
       
        <!--The buffer size in bytes for sorting.  The following is the recommended configuration.-->
        <property>
                <name>hive.algebricks.sort.memory</name>
                <value>33554432</value>
        </property>
```

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

# Getting help #
If you encounter a problem, please send an email to hivesterix-users@googlegroups.com

## Report bugs ##
If you find a bug, please send an email to hivesterix-users@googlegroups.com, or open an issue in http://code.google.com/p/hyracks/issues/list.