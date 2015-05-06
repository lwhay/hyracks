### Feature Notes ###
This is the first release of Pregelix, which supports a major subset of the standard Pregelix API, including:

-- Vertex API

-- Send/receive messages

-- Message combiner

-- Global Aggregator

-- Graph Mutations

### New Features ###
|Feature|Description|
|:------|:----------|
| **Graph topological mutations** |adding/removing vertex is supported.|
| **Variable length states** | variable length vertex states is supported.|
| **Various HDFS versions** | supported HDFS versions include hadoop-0.20.2, hadoop-0.23.1, hadoop-0.23.6, hadoop-1.0.4, CDH-4.1, CDH-4.2 |
| **Better HDFS scheduling** | HDFS operations work well for the case where replication factor is larger than 1 and the data are not balanced.|
| **Less memory copies** |removed unnecessary memory copies substantially|
| More examples |new examples like triangle counting, maximal clique and graph mutation are added.|

### Issue Notes ###
Issues fixed in this releases:

[Issue 85](http://code.google.com/p/hyracks/issues/detail?id=85): Support graph toplogical mutations.

[Issue 86](http://code.google.com/p/hyracks/issues/detail?id=86): Support vertex value size change.