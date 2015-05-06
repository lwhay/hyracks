# Introduction #

This page describes the code required to create a very simple job. All this job does is read a CSV file and print the lines to the console. This simple example can also be found in the test suite at: http://code.google.com/p/hyracks/source/browse/tags/hyracks-0.1.4/hyracks-examples/hyracks-integration-tests/src/test/java/edu/uci/ics/hyracks/tests/integration/ScanPrintTest.java

# Creating a Job Specification #

First, we create a new JobSpecification object.

```
    public void scanPrint01() throws Exception {
        JobSpecification spec = new JobSpecification();
```

This job will have two operators:
  1. A File Scan Operator to scan a CSV File
  1. A Printer Operator


The first argument to the File Scanner is the Job Specification object that this operator will be a part of. The second argument is a Split Provider that indicate to the operator the location of the partitions of the file it is supposed to read.
In this example, we create an array with two splits

```
        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(new FileSplit[] {
                new FileSplit(NC2_ID, new FileReference(new File("data/words.txt"))),
                new FileSplit(NC1_ID, new FileReference(new File("data/words.txt"))) });
```

The third argument to the File Scan Operator is the Tuple Parser Factory implementation that is responsible for creating a Tuple Parser that has the ability to convert the bytes in the file to tuples. We use a DelimitedDataTupleParserFactory in this case.

The last argument to the File Scan Operator is a Record descriptor. In general, a record descriptor describes to Hyracks the number of fields in a tuple stream and the mechanism to serialize and deserialize the fields. In this case, the record descriptor object describes the output of the CSV File Scan Operator, and specifies that there is one field in the tuple (indicated by the argument array's size of one item), and that one field is to be serialized and deserialized using a StringSerializerDeserializer. Look at **TODO** to see a list of ISerializerDeserializer implementations that are packaged with Hyracks.

```
        RecordDescriptor desc = new RecordDescriptor(
                new ISerializerDeserializer[] { StringSerializerDeserializer.INSTANCE });
```

Now we create the File Scan Operator.

```
        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
                spec,
                splitProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE }, ','),
                desc);
```

Currently, every operator in Hyracks needs a partition constraint specified to indicate how many instances of this operator should be created at runtime and where in the cluster they should be placed. In this example, we specify that two instances of the CSV File Scan Operator need to be created and they should be placed at Hyracks nodes with ids NC2\_ID and NC1\_ID respectively.

```
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID, NC1_ID);
```

Similarly we create the Printer Operator and partition it similarly to the file scanners.
```
        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC2_ID, NC1_ID);
```

The operators constructed have to be connected using a Connector that describes how data should be moved from the producer-operator's partitions to the consumer-operator's partitions. In this example we connect the CSV File Scan Operator to the Printer Operator using a OneToOneConnector. As the name suggests, the OneToOneConnector connects each producer partition to one consumer partition.

```
        IConnectorDescriptor conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn, csvScanner, 0, printer, 0);
```

Finally, we add the roots of the DAG to the Job Specification, and run the test.
```
        spec.addRoot(printer);
        runTest(spec);
    }
```

The code to run the test as follows. This code assumes that you have already obtained a handle to the IClusterController which accepts your jobs.

```
    void runTest(JobSpecification spec) throws Exception {
        UUID jobId = cc.createJob(spec);
        cc.start(jobId);
        cc.waitForCompletion(jobId);
    }
```

The following code can be used to get a handle to the IClusterController object.

```
Registry registry = LocateRegistry.getRegistry(ccHost, ccPort);
IClusterController cc = registry.lookup(IClusterController.class.getName());
```