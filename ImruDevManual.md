### Introduction ###
IMRU is an open-source implementation of the [Iterative Map Reduce Update](http://arxiv.org/abs/1203.0160) which runs on top of the Hyracks general-purpose query execution engine. This document introduces how to develop IMRU applications. The following figure shows how data flows inside IMRU

![http://hyracks.googlecode.com/svn/wiki/images/imru/imruLogical.png](http://hyracks.googlecode.com/svn/wiki/images/imru/imruLogical.png)

To use IMRU, we need to define three Serializable objects and provides an implementation of [IIMRUJob interface](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/imru/imru-core/src/main/java/edu/uci/ics/hyracks/imru/api/IIMRUJob.java).

### Data Objects ###

IMRU framework needs three different data objects. Each of them is a user defined serializable java object.

#### Training Example ####
Each training example is java object designed to store information related to a training example. It is generated in user defined function **parse()** and used as an input in user defined function **map()**.
An training example of 2D-KMeans can be found [here](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/imru/imru-example/src/main/java/edu/uci/ics/hyracks/imru/example/kmeans/DataPoint.java).

#### Machine Learning Model ####
Each machine learning model is java object designed to store information related to a machine learning model. It is generated in user defined function **initModel()** and used as the input of user defined function **map()** and **update()**.
The model of 2D-KMeans can be found [here](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/imru/imru-example/src/main/java/edu/uci/ics/hyracks/imru/example/kmeans/KMeansModel.java).

#### Intermediate Result ####
Each intermediate result is java object designed to store information related to how to update the machine learning model. It is generated in user defined function **map()** and **reduce()**, and used as the input of user defined function **reduce()** and **update()**.
The intermediate result of 2D-KMeans can be found [here](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/imru/imru-example/src/main/java/edu/uci/ics/hyracks/imru/example/kmeans/KMeansCentroids.java).

### User Defined Functions ###
Besides defining the three different types of data objects, we also need to provide an implementation of [IIMRUJob interface](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/imru/imru-core/src/main/java/edu/uci/ics/hyracks/imru/api/IIMRUJob.java). IIMRUJob contains the following user defined functions.

| Function | Description |
|:---------|:------------|
| initModel() | Return the initial model. |
| getCachedDataFrameSize() | Return the size of data frame passed between machines. It must be larger enough to hold at least one data example. |
| parse() | The input is the input stream of one input file. The output is data objects. |
| map() | The input is an iterator of data objects. The output is one map result. |
| reduce() | The input is an iterator of map results. The output is one reduce result |
| update() | The input is an iterator of reduce results. This function should update the model.  |
| shouldTerminate() | This function is called after **update()**. It decides whether there is another iteration. |

### Implementing the first example ###
This section guides throught the [K-Means](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/imru/imru-example/src/main/java/edu/uci/ics/hyracks/imru/example/kmeans/) example.
This is a very simple example that perform K-Means clustering with 2 dimensional data points.

**Note**: Beside K-Means, Other examples are also available. The following examples can be found in IMRU example package
| Example | Description |
|:--------|:------------|
| [K-Means](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/imru/imru-example/src/main/java/edu/uci/ics/hyracks/imru/example/kmeans/) |  K-Means clustering with 2 dimensional data points |
| [batch gradient descent](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/imru/imru-example/src/main/java/edu/uci/ics/hyracks/imru/example/bgd) | Batch Gradient Descent|
| [Helloworld](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/imru/imru-example/src/main/java/edu/uci/ics/hyracks/imru/example/helloworld) | A simple IMRU application prints out the data flow|


#### Development environment ####
Please follow [this guide](http://code.google.com/p/hyracks/wiki/ImruUserManual#Build_IMRU) to download IMRU and setup a eclipse development environment.

#### Code for the Entry ####

In eclipse imru-example project, open and run [src/main/java/edu/uci/ics/hyracks/imru/example/kmeans/KMeans.java](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/imru/imru-example/src/main/java/edu/uci/ics/hyracks/imru/example/kmeans/KMeans.java)

**Note**: `KMeans.java` will

1. setup a local cluster controller running in the same process as KMeansDebug

2. setup two local node controllers named "nc1" and "nc2" running in the same process as KMeansDebug

3. generate a HAR will all code and upload it to the cluster

4. run the job and print the result

### Executing the first example ###

Similarly we can run the example distributedly as described [here](http://code.google.com/p/hyracks/wiki/ImruUserManual#Running_IMRU_in_a_cluster).