# Your first Hyracks Example #

This document describes how to:

  * 0. Get Hyracks
  * 1. Start Hyracks on one machine in a "virtual" cluster configuration.
  * 2. Deploy a Hyracks Application onto the Cluster Controller.
  * 3. Run a simple word count example.

## Step 0: Getting Hyracks ##

Use SVN to get the latest release of Hyracks. If you are using the command-line SVN client, you can run:

svn co http://hyracks.googlecode.com/svn/tags/hyracks-<latest version>

Make sure to replace <latest version> in the URL (and in all mentions below) with the latest version mentioned on the main page of the project.

Follow the build instructions mentioned in the QuickStartGuide.

When you see "BUILD SUCCESSFUL" you know that you have successfully built Hyracks.

## Step 1: Start a "virtual" Hyracks Cluster ##

Start the Cluster Controller by running:

```
hyracks-server/target/hyracks-server-<latest version>-binary-assembly/bin/hyrackscc -cluster-net-ip-address 127.0.0.1 -client-net-ip-address 127.0.0.1
```

It is normal to see a failed message of the following kind.

```
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
```

When you see

```
INFO: Started ClusterControllerService
```

it means that the Cluster Controller has been successfully started.

Start a Node Controller (in another console window).

```
hyracks-server/target/hyracks-server-<latest release>-binary-assembly/bin/hyracksnc -cluster-net-ip-address 127.0.0.1 -cc-host localhost -data-ip-address 127.0.0.1 -node-id NC1
```
You should see a message saying the node controller has been started and the window with the Cluster Controller should show heartbeat messages from NC1.

Start one more Node Controller (in yet another console window).

```
hyracks-server/target/hyracks-server-<latest release>-binary-assembly/bin/hyracksnc -cluster-net-ip-address 127.0.0.1 -cc-host localhost -data-ip-address 127.0.0.1 -node-id NC2
```

You should see the same behavior as above.

At this time you have successfully started a Cluster Controller and 2 Node Controllers to create a virtual Hyracks cluster.

## Step 2: Deploy a Hyracks application ##

In a new console window run the script at:

```
hyracks-cli/target/hyracks-cli-<latest release>-binary-assembly/bin/hyrackscli
```

You should see a "hyracks>" prompt.

First we connect to the Cluster Controller with the following command (hyracks> is the prompt -- Do not type that):

```
hyracks> connect to "localhost";
```

At this time we are ready to deploy a Hyracks application using the Hyracks command-line client.

The application we are going to deploy as part of this exercise is the "text-example" that can be found in following zip file. Obtain the absolute path to this zip file for the next step.
```
hyracks-examples/text-example/textapp/target/textapp-<latest version>-app-assembly.zip
```

Issue the following command at the CLI prompt:

```
create application text "<absolute path to zip file>";
```
Replace <absolute path to zip file> with the absolute path to the zip file mentioned above.

When this command returns, the application has been successfully deployed in the Hyracks Cluster Controller.

## Step 3: Run the word count example ##

Now we are ready to run the word count example. Before we do so, we need some data files which we will use for the example. There are some data files in the hyracks-example/hyracks-integration-tests/data folder. You could also decide to use your own data files.

We can provide one or more input files (whose words will be counted). In this example, we will have NC1 read the input files and have NC2 produce the output. Let's say 

&lt;df1&gt;

 and 

&lt;df2&gt;

 are the absolute filenames of the two data files you wish to use for this example, and let's say we want the output at /tmp/output

From another console window, run the following script:

```
hyracks-examples/text-example/textclient/target/textclient-<latest version>-binary-assembly/bin/textclient -host localhost -app text -infile-splits "NC1:<df1>,NC1:<df2>" -outfile-splits "NC2:/tmp/output" -algo hash
```
Be sure to replace 

&lt;df1&gt;

 and 

&lt;df2&gt;

 with the absolute filenames of the input data files.

The above script will run the word count example on the given data files and write the output to /tmp/output.