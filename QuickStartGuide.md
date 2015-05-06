# Quick Start Guide #

## Running Hyracks ##

Hyracks is made up of two parts that need to run on a cluster to accept and run users' jobs. The Hyracks Cluster Controller must be run on one machine designated as the
master node. This machine should be able to be accessed from the other nodes in the cluster that would do work as well as the client machines that would submit jobs to Hyracks.
The worker nodes (machines) must run a Hyracks Node Controller.

### Starting the Hyracks Cluster Controller ###

The simplest way to start the cluster controller is to run bin/hyrackscc.

` bin/hyrackscc -client-net-ip-address <client-reachable-ip-address> -cluster-net-ip-address <cluster-reachable-ip-address> `

In the above setting, substitute the IP addresses with the IP address of the machine's network interface where
you wish to run the cluster controller. In case the machine has multiple network
interfaces, use an IP address for the cluster-net-ip-address that is reachable by the machines that will
run the node controllers, and the client-net-ipaddress with the IP address that is reachable from the clients that will connect to the cluster controller.

### Starting the Hyracks Node Controller ###

The node controller is started by running bin/hyracksnc. It requires at least the following three command line arguments.

```
-cluster-net-ip-address   : IP Address that is reachable from the ClusterController
-node-id VAL                   : Unique name for this node controller
-cc-host VAL                    : Cluster Controller host name
-data-ip-address VAL            : IP Address to bind data listener
```

If the cluster controller was directed to listen on a port other than the default, you will need to pass one more argument to hyracksnc.

` -cc-port N                      : Cluster Controller port (default: 1099) `

The data-ip-address is the interface on which the Node Controller must listen on -- in the event the machine is multi-homed it must listen on an IP that is reachable from
other Node Controllers. Make sure that the value passed to the data-ip-address is a valid IPv4 address (four octets separated by .).

## Building Hyracks from source ##

### Prerequisites ###

  1. JDK 1.6
  1. Maven2 (maven.apache.org)

### Steps ###

  1. Checkout Hyracks from http://hyracks.googlecode.com/svn/tags/hyracks-{latest-version} using an SVN client
  1. cd into the hyracks-{latest-version} folder.
  1. Run ` mvn package `
  1. That's it!

## Importing the Hyracks Project into Eclipse ##

### Prerequisites ###

You will need to have the Eclipse Maven plugin (http://m2eclipse.sonatype.org/)

### Steps ###

  1. Open eclipse
  1. Right click in the Package Explorer pane.
  1. Click on Import...
  1. Under "Maven" choose "Existing Maven Projects"
  1. Pick the project root option and browse to the hyracks-{latest-version} folder
  1. Select all the project pom.xml files that are found
  1. Click on Finish

Note: The hyracks-cli project will be marked with a red X indication compilation problems. This problem is due to the fact that hyracks-cli needs the regular Maven build to create some additional Java files. Make sure that the steps mentioned in "Building Hyracks from source" have been followed. Select the hyracks-cli project in the Eclipse Package Explorer and refresh its contents (Right-click and choose Refresh, or press F5). Once the refresh has completed, navigate to target/generated-sources/javacc, right click on javacc. Under the "Build Path" menu item, choose "Use as Source Folder".

## Developing and running applications for Hyracks ##

## Maven Setup ##

Hyracks uses Maven to handle dependency management and other aspects of the project lifecycle. The best way to start building an application using Hyracks is to use Maven to manage your application project. For more information on how to use Maven please look at resources at (http://maven.apache.org/).

Once you create your POM for your application, add the following to your project:

```
<project ...>
<repositories>
    <repository>
      <id>Hyracks Repository</id>
      <url>http://obelix.ics.uci.edu/nexus/content/groups/hyracks-public/</url>
    </repository>
 </repositories>
</project>
```

The tpch-example project under hyracks-examples contains a complete example of how to
create the packaging for a job that will be executed on a Hyracks cluster.

As a best practice, package all the components that will be deployed on the cluster into a separate library project (e.g. the tpchhelper project under tpch-example). Create another project that builds a deployable artifact (e.g. the tpchapp project under tpch-example). Finally, make your client project depend on the library project so that you can instantiate and use the classes in your library project (e.g. the tpchclient project under tpch-example).