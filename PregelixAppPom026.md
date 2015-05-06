<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
> 

&lt;modelVersion&gt;

4.0.0

&lt;/modelVersion&gt;


> 

&lt;groupId&gt;

edu.uci.ics.pregelix

&lt;/groupId&gt;


> 

&lt;artifactId&gt;

neighbor-counting

&lt;/artifactId&gt;


> 

&lt;version&gt;

0.2.6

&lt;/version&gt;


> 

&lt;packaging&gt;

jar

&lt;/packaging&gt;


> 

&lt;name&gt;

neighbor-counting

&lt;/name&gt;



> 

&lt;properties&gt;


> > <jvm.extraargs/>

> 

&lt;/properties&gt;



> 

&lt;profiles&gt;


> > 

&lt;profile&gt;


> > > 

&lt;id&gt;

macosx

&lt;/id&gt;


> > > 

&lt;activation&gt;


> > > > 

&lt;os&gt;


> > > > > 

&lt;name&gt;

mac os x

&lt;/name&gt;



> > > > 

&lt;/os&gt;


> > > > 

&lt;jdk&gt;

1.7

&lt;/jdk&gt;



> > > 

&lt;/activation&gt;


> > > 

&lt;properties&gt;


> > > > <jvm.extraargs>-Djava.nio.channels.spi.SelectorProvider=sun.nio.ch.KQueueSelectorProvider</jvm.extraargs>

> > > 

&lt;/properties&gt;



> > 

&lt;/profile&gt;



> 

&lt;/profiles&gt;



> 

&lt;build&gt;


> 

&lt;plugins&gt;


> > 

&lt;plugin&gt;


> > 

&lt;groupId&gt;

org.apache.maven.plugins

&lt;/groupId&gt;


> > 

&lt;artifactId&gt;

maven-compiler-plugin

&lt;/artifactId&gt;


> > 

&lt;version&gt;

2.0.2

&lt;/version&gt;


> > 

&lt;configuration&gt;


> > > 

&lt;source&gt;

1.6

&lt;/source&gt;


> > > 

&lt;target&gt;

1.6

&lt;/target&gt;



> > 

&lt;/configuration&gt;


> > 

&lt;/plugin&gt;


> > 

&lt;plugin&gt;


> > > 

&lt;artifactId&gt;

maven-assembly-plugin

&lt;/artifactId&gt;


> > > 

&lt;configuration&gt;


> > > > 

&lt;descriptorRefs&gt;


> > > > > 

&lt;descriptorRef&gt;

jar-with-dependencies

&lt;/descriptorRef&gt;



> > > > 

&lt;/descriptorRefs&gt;



> > > 

&lt;/configuration&gt;


> > > 

&lt;executions&gt;


> > > > 

&lt;execution&gt;


> > > > > 

&lt;id&gt;

make-my-jar-with-dependencies

&lt;/id&gt;


> > > > > 

&lt;phase&gt;

package

&lt;/phase&gt;


> > > > > 

&lt;goals&gt;


> > > > > > 

&lt;goal&gt;

single

&lt;/goal&gt;



> > > > > 

&lt;/goals&gt;



> > > > 

&lt;/execution&gt;



> > > 

&lt;/executions&gt;



> > 

&lt;/plugin&gt;


> > 

&lt;plugin&gt;


> > > 

&lt;groupId&gt;

org.apache.maven.plugins

&lt;/groupId&gt;


> > > 

&lt;artifactId&gt;

maven-surefire-plugin

&lt;/artifactId&gt;


> > > 

&lt;version&gt;

2.7.2

&lt;/version&gt;


> > > 

&lt;configuration&gt;


> > > > 

&lt;forkMode&gt;

pertest

&lt;/forkMode&gt;


> > > > 

&lt;argLine&gt;

-enableassertions -Xmx2047m -Dfile.encoding=UTF-8
> > > > > -Djava.util.logging.config.file=src/test/resources/logging.properties

&lt;/argLine&gt;



> > > > 

&lt;includes&gt;


> > > > > 

&lt;include&gt;

/**TestSuite.java

&lt;/include&gt;


> > > > >**

&lt;include&gt;

/**Test.java

&lt;/include&gt;



> > > > 

&lt;/includes&gt;



> > > 

&lt;/configuration&gt;



> > 

&lt;/plugin&gt;


> >**

&lt;plugin&gt;


> > > 

&lt;artifactId&gt;

maven-clean-plugin

&lt;/artifactId&gt;


> > > 

&lt;configuration&gt;


> > > > 

&lt;filesets&gt;


> > > > > 

&lt;fileset&gt;


> > > > > > 

&lt;directory&gt;

.

&lt;/directory&gt;


> > > > > > 

&lt;includes&gt;


> > > > > > > 

&lt;include&gt;

teststore**&lt;/include&gt;


> > > > > > >**

&lt;include&gt;

edu**&lt;/include&gt;


> > > > > > >**

&lt;include&gt;

actual**&lt;/include&gt;


> > > > > > >**

&lt;include&gt;

build**&lt;/include&gt;


> > > > > > >**

&lt;include&gt;

expect**&lt;/include&gt;


> > > > > > >**

&lt;include&gt;

ClusterController**&lt;/include&gt;


> > > > > > >**

&lt;include&gt;

edu.uci.**&lt;/include&gt;



> > > > > > 

&lt;/includes&gt;



> > > > > 

&lt;/fileset&gt;



> > > > 

&lt;/filesets&gt;



> > > 

&lt;/configuration&gt;



> > 

&lt;/plugin&gt;


> > 

&lt;/plugins&gt;



> 

&lt;/build&gt;**

> 

&lt;dependencies&gt;


> > 

&lt;dependency&gt;


> > > 

&lt;groupId&gt;

edu.uci.ics.hyracks

&lt;/groupId&gt;


> > > 

&lt;artifactId&gt;

pregelix-core

&lt;/artifactId&gt;


> > > 

&lt;version&gt;

0.2.6

&lt;/version&gt;


> > > 

&lt;type&gt;

jar

&lt;/type&gt;


> > > 

&lt;scope&gt;

compile

&lt;/scope&gt;



> > 

&lt;/dependency&gt;


> > > 

&lt;dependency&gt;


> > > 

&lt;groupId&gt;

edu.uci.ics.hyracks

&lt;/groupId&gt;


> > > 

&lt;artifactId&gt;

pregelix-example

&lt;/artifactId&gt;


> > > 

&lt;version&gt;

0.2.6

&lt;/version&gt;


> > > 

&lt;type&gt;

jar

&lt;/type&gt;


> > > 

&lt;scope&gt;

compile

&lt;/scope&gt;



> > 

&lt;/dependency&gt;


> > 

&lt;dependency&gt;


> > > 

&lt;groupId&gt;

junit

&lt;/groupId&gt;


> > > 

&lt;artifactId&gt;

junit

&lt;/artifactId&gt;


> > > 

&lt;version&gt;

4.8.1

&lt;/version&gt;


> > > 

&lt;scope&gt;

test

&lt;/scope&gt;



> > 

&lt;/dependency&gt;



> 

&lt;/dependencies&gt;



> 

&lt;scm&gt;


> > 

&lt;connection&gt;

scm:svn:https://hyracks.googlecode.com/svn/trunk/fullstack/pregelix

&lt;/connection&gt;


> > 

&lt;developerConnection&gt;

scm:svn:https://hyracks.googlecode.com/svn/trunk/fullstack/pregelix

&lt;/developerConnection&gt;


> > 

&lt;url&gt;

http://code.google.com/p/hyracks/source/browse/#svn/trunk/fullstack/pregelix

&lt;/url&gt;



> 

&lt;/scm&gt;



> 

&lt;distributionManagement&gt;


> > 

&lt;repository&gt;


> > > 

&lt;id&gt;

hyracks-releases

&lt;/id&gt;


> > > 

&lt;url&gt;

http://obelix.ics.uci.edu/nexus/content/repositories/hyracks-releases/

&lt;/url&gt;



> > 

&lt;/repository&gt;


> > 

&lt;snapshotRepository&gt;


> > > 

&lt;id&gt;

hyracks-snapshots

&lt;/id&gt;


> > > 

&lt;url&gt;

http://obelix.ics.uci.edu/nexus/content/repositories/hyracks-snapshots/

&lt;/url&gt;



> > 

&lt;/snapshotRepository&gt;



> 

&lt;/distributionManagement&gt;



> 

&lt;reporting&gt;


> > 

&lt;plugins&gt;


> > > 

&lt;plugin&gt;


> > > > 

&lt;groupId&gt;

org.apache.maven.plugins

&lt;/groupId&gt;


> > > > 

&lt;artifactId&gt;

maven-changelog-plugin

&lt;/artifactId&gt;



> > > 

&lt;/plugin&gt;



> > 

&lt;/plugins&gt;



> 

&lt;/reporting&gt;



> 

&lt;repositories&gt;


> > 

&lt;repository&gt;


> > > 

&lt;id&gt;

hyracks-public

&lt;/id&gt;


> > > 

&lt;url&gt;

http://obelix.ics.uci.edu/nexus/content/groups/hyracks-public/

&lt;/url&gt;



> > 

&lt;/repository&gt;


> > 

&lt;repository&gt;


> > > 

&lt;id&gt;

jboss-public

&lt;/id&gt;


> > > 

&lt;url&gt;

https://repository.jboss.org/nexus/content/groups/public/

&lt;/url&gt;



> > 

&lt;/repository&gt;



> 

&lt;/repositories&gt;



> 

&lt;pluginRepositories&gt;


> > 

&lt;pluginRepository&gt;


> > > 

&lt;id&gt;

hyracks-public

&lt;/id&gt;


> > > 

&lt;url&gt;

http://obelix.ics.uci.edu/nexus/content/groups/hyracks-public/

&lt;/url&gt;


> > > 

&lt;releases&gt;


> > > > 

&lt;updatePolicy&gt;

always

&lt;/updatePolicy&gt;



> > > 

&lt;/releases&gt;



> > 

&lt;/pluginRepository&gt;



> 

&lt;/pluginRepositories&gt;




Unknown end tag for &lt;/project&gt;

  }}}```