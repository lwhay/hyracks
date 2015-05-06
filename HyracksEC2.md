## Introduction ##
Hyracks EC2 package is used to deploy a cluster using Amazon AWS service.

### Getting hyracks EC2 package ###
1. Download IMRU:
```
$cd ~/
$svn co https://hyracks.googlecode.com/svn/branches/fullstack_imru
```

2. Build:
```
cd fullstack_imru
export MAVEN_OPTS=-Xmx512m
mvn package -DskipTests=true
chmod -R 755 hyracks/hyracks-ec2/target/appassembler/bin/*
```
Proceed only when you see "BUILD SUCCESSFUL" on your screen after "mvn package -DskipTests=true". The build needs to download files over the network. It may take several minutes to an hour depending on your network speed. Note that the "chmod -R 755 ." command is to make all the binaries executable.


### AWS credientail and keys ###
[fullstack\_imru/hyracks/hyracks-ec2/target/appassembler/conf/ec2.properties](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/hyracks/hyracks-ec2/src/main/resources/conf/ec2.properties) contains a list of parameters for the EC2 package. The first two parameters must be taken care of to use AWS service. Either modify them or place the required file in the specified location.

|Name|Sample Value| Explanation |
|:---|:-----------|:------------|
|CREDENTIALS\_FILE|$USER\_HOME/AwsCredentials.properties|Pointing to a file which contains access key and secret key downloaded from http://aws.amazon.com/security-credentials|
|KEY\_FILE|$USER\_HOME/firstTestByRui.pem|Private key file - downloaded from https://console.aws.amazon.com/ec2/ Key pair section|
|MAXIMUM\_INSTANCES|5 |Maximum instances allowed to run (specify this to avoid accidentally start a large amount of instances)|
|CLUTER\_PREFIX|hyracks-auto-deploy-|Unique prefix of each instance name belong to this cluster|
|AMI\_ID|ami-5eb02637|Snapshot used to create instance|
|INSTANCE\_TYPE|t1.micro|Instance type of newly created instance|
|SECURITY\_GROUP|hyracks-security-group|security group of new instances (created automatically)|
|OPENED\_TCP\_PORTS|22,1099,3099,16001|opened tcp ports|

Sample content of [AwsCredentials.properties](http://code.google.com/p/hyracks/source/browse/branches/fullstack_imru/hyracks/hyracks-ec2/src/main/resources/AwsCredentials.properties):
```
accessKey=xx
secretKey=xx
```

### Command line ###
1. Locate executable files:
```
cd ~/fullstack_imru/hyracks/hyracks-ec2/target/appassembler/bin
```

2. Getting help
```
bin/ec2
```

3. Available commands

| s|status                     | print status of all instances|
|:-----------------------------|:-----------------------------|
| sic|setInstanceCount `<count>` | add/remote instances to reach `<count>`|
| addi|addInstances `<count>`    | add `<count>` instances|
| install                      | install hyracks to all instances|
| sti|startInstances           | start all instances|
| sth|startHyrack              | start hyracks on all instances|
| sph|stopHyracks              | stop hyracks on all instances|
| spi|stopInstances            | stop all instances|
| termi|terminateInstances     | terminate all instances|
| logs                         | show hyracks logs on all instances|
| logs `<id>`                   | show hyracks logs of one instance|

### Example Commands ###
```
./ec2 s; #show status
./ec2 sic; #start two instances, a hyracks security group maybe created if it's not exists
./ec2 s; #check status until all instances are running
./ec2 install; #synchronized latested hyracks code to all instances
./ec2 sth; #start hyracks cluster
./ec2 s; #check Admin URL, open it in a browser
./ec2 logs; #print out all logs (if things go wrong)
./ec2 sph; #stop hyracks cluster
./ec2 spi; #stop all instances
./ec2 termi; #terminate all instances
```