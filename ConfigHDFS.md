We assume you have a NFS installed in the cluster. Otherwise, you need to copy the hadoop binary to every machine, with the same path.

1. Download hadoop-0.20.2 and unzip:
Install wget if you are using a Mac:
http://osxdaily.com/2012/05/22/install-wget-mac-os-x/. Then, do the following:
```
cd ~/
wget http://archive.apache.org/dist/hadoop/core/hadoop-0.20.2/hadoop-0.20.2.tar.gz .
tar -xvf hadoop-0.20.2.tar.gz
cd hadoop-0.20.2
```

2. Configure master/slaves:

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


3. Configure properties:

Edit conf/core-site.xml, for example: [core-site.xml](http://code.google.com/p/hyracks/wiki/CoreSiteXml)

Edit conf/hdfs-site.xml, for example: [hdfs-site.xml](http://code.google.com/p/hyracks/wiki/HDFSSiteXml)

Note that the above example configuration files are just templates, and you **need to customize that according to your own system**. Here are the important properties that you **must edit**:

|Property|File|Meaning|
|:-------|:---|:------|
|fs.default.name|core-site.xml|The HDFS URL.|
|dfs.datanode.port|hdfs-site.xml|The HDFS datanode port on each slave machine.|
|dfs.datanode.ipc.address|hdfs-site.xml|The HDFS datanode IPC address on each slave node.|
|dfs.info.port|hdfs-site.xml|The HDFS datanode info port on each slave machine.|
|dfs.secondary.info.port|hdfs-site.xml|The HDFS datanode secondary info port on each slave machine.|
|dfs.replication|hdfs-site.xml|The replication factory of HDFS, e.g., how many copies of each file should exist in the distributed file system.|
|dfs.block.size|hdfs-site.xml|The block size in HDFS.|
|dfs.data.dir|hdfs-site.xml|Comma separated data directories on each datanode, usually one-per-disk-drive, for disk stripping.|

**Edit conf/hadoop-env.sh to change the logging directory** to use an existing local file system path instead of a NFS path, for example:
```
export HADOOP_LOG_DIR=/grid/0/dev/vborkar/logs
```
Also, set the right JAVA\_HOME in conf/hadoop-env.sh.

4. Start the Hadoop cluster:
```
bin/hadoop namenode -format
bin/start-dfs.sh
```

5. **Optional**: shutdown the Hadoop cluster when you want to:
```
bin/stop-dfs.sh
```