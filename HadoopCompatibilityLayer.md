Hyracks offers a Hadoop compatibility layer that allows a Hadoop Map-Reduce job to run on Hyracks as it is without any modifications. This page is a tutorial for using the Hadoop compatibility Layer.

# Introduction #

Map-Reduce has become a popular computational model as is indicated by the wide deployment of Hadoop, an open source framework capable of running a Map-Reduce job in a distributed environment. A Map-Reduce job, being a data flow graph can be executed on Hyracks. The Hadoop compatibility layer enables users to run existing Hadoop Map-Reduce jobs unchanged on Hyracks as an initial ”get acquainted” strategy as well as a migration strategy for ”legacy” data intensive applications. The input/output characteristics of the job remain the same, with both input and output residing in the Hadoop distributed file system (HDFS).

## Getting Started with the Hadoop compatibility layer ##

### Installation and Configuration ###

#### Requirements ####
  * Java 1.6
  * Hadoop 0.20.x
  * Hyracks 0.1.6 or greater

Start by downloading the Hadoop compatibility layer archive. Next you need to unpack the tarball. This will result in creation of a directory: hadoop\_compatibility.

```
 $ tar -xzvf hadoop_compatibility.tar.gz
```

Set the environment variable "MY\_COMPAT\_HOME" to point to this  directory.

```
 $ cd hadoop_compatibility
 $ export MY_COMPAT_HOME=`pwd`
```


### Running a Hadoop Job using the compatibility layer ###

#### Step 1: Configuring the Hadoop comaptibility layer ####

If you have built hyracks on your local machine, then  set the environment variable MY\_HYRACKS\_HOME to the build directory.

```
 export MY_HYRACKS_HOME={hyracks_build_directory}
```

You need not worry if you have not previously built hyracks on your local machine. Just continue with the remaining set of instructions.

Navigate to the $MY\_COMPAT\_HOME directory and execute the set-env.sh script as shown below.

```
$cd $MY_COMPAT_HOME
$ chmod +x ./scripts/set-env.sh
$ ./scripts/set-env.sh {hyracks_version} {hyracks_cc_host} {hdfs namenode URL}
```

  * hyracks\_version: The hyracks version you are using. For example 0.1.7, 0.1.8 etc.
  * hyracks\_cc\_host: The address of the machine running the hyracks cluster controller. You can use "localhost" as well, if the hyracks cluster controller is running on your local machine.
  * hdfs namenode URL: The URL of the HDFS namenode. For example, if the HDFS namenode is running on localhost and listening on port 9000, you should use hdfs://localhost:9000


#### Step 2: Describing a Map-Reduce job ####

  * Creating a job file

A Map-Reduce job is expressed as a set of key-value pairs where each
pair represents a job parameter. A Hadoop end-user is required to set the job  parameters in a JobConf instance and submit the instance to a Hadoop JobClient. When using the Hadoop compatibility layer, an end-user is required to form a _job_ file that contains the job parameters written as key-value pairs.

An example job file is shown below. The job described by this example job file represents a Map-Reduce job that computes the word frequencies for the content contained in the input data/file1.txt. The result is placed in a file named 'output'.

```
mapred.input.dir=data/file1.txt
mapred.output.dir=output
mapred.job.name=wordcount
mapred.mapper.class=edu.uci.ics.hyracks.examples.wordcount.WordCount$Map
mapred.combiner.class=edu.uci.ics.hyracks.examples.wordcount.WordCount$Reduce
mapred.reducer.class=edu.uci.ics.hyracks.examples.wordcount.WordCount$Reduce
mapred.input.format.class=org.apache.hadoop.mapred.TextInputFormat
mapred.output.format.class=org.apache.hadoop.mapred.TextOutputFormat
mapred.mapoutput.key.class=org.apache.hadoop.io.Text
mapred.mapoutput.value.class=org.apache.hadoop.io.IntWritable
mapred.output.key.class=org.apache.hadoop.io.Text
mapred.output.value.class=org.apache.hadoop.io.IntWritable
```

#### Step 3: Submitting a Map-Reduce job ####

The job file constructed in Step 2 is submitted to the Hadoop compatibility layer using a script file - submit\_job.sh (located at $MY\_COMPAT\_HOME/scripts/submit\_job.sh)

```
$ cd $MY_COMPAT_HOME
$ ./scripts/submit_job.sh {application_name} {path to job file (from step 2)} {comma separated list (containing path) of one or more jar files that are required by the Map-Reduce job.}
```

Above, {application\_name} is an identifier used by the compatibility layer to form a Hyracks application. You can use any name, for example if you are executing a Map-Reduce job that does a word count, you may choose "wordcount".


#### Step 4: Viewing the output ####

The result of the submitted Map-Reduce job is located at the path specified by the parameter "mapred.output.dir" defined in the job file. You can view the output by using

```
$ $HADOOP_HOME/bin/hadoop fs -cat {mapred.output.dir}
```


### Other Features ###

#### Pipelining multiple Map-Reduce jobs ####

If you have multiple Map-Reduce jobs such that they form a sequence wherein output from one Map-Reduce job is fed as input to another and so on, then it is efficient to execute the sequence of Map-Reduce jobs as a single Hyracks job. The Hadoop compatibility layer can translate a sequence of Map-Reduce job into a single Hyracks job.

In order to use this feature, you need to do the following.

a) Form the respective job files (as described in Step 2) corresponding to each Map-Reduce job int the sequence.

b) For submitting the sequence of Map-Reduce job, use a comma separated list of the job files formed in step (a) as an argument to
submit\_job.sh bash script. Note that the order used in the list forms the order of jobs in the formed pipeline.

```
$ cd $MY_COMPAT_HOME
$ $MY_COMPAT_HOME/scripts/submit_job.sh {application_name} {comma separated list of job files} {comma separated list of respective paths to required jar files}
```