<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->



&lt;configuration&gt;



> 

&lt;property&gt;


> > 

&lt;name&gt;

mapred.system.dir

&lt;/name&gt;


> > 

&lt;value&gt;

/grid/0/dev/vborkar/hadoop-rack345-1-mrsys

&lt;/value&gt;



> 

&lt;/property&gt;



> 

&lt;property&gt;


> > 

&lt;name&gt;

mapred.local.dir

&lt;/name&gt;


> > 

&lt;value&gt;

/grid/0/dev/vborkar/hadoop-rack345-1-tmp1,/grid/1/dev/vborkar/hadoop-rack345-1-tmp1,/grid/2/dev/vborkar/hadoop-rack345-1-tmp1,/grid/3/dev/vborkar/hadoop-rack345-1-tmp1

&lt;/value&gt;



> 

&lt;/property&gt;



> 

&lt;property&gt;


> > 

&lt;name&gt;

mapred.job.tracker

&lt;/name&gt;


> > 

&lt;value&gt;

xyz.com:29007

&lt;/value&gt;



> 

&lt;/property&gt;


> 

&lt;property&gt;


> > 

&lt;name&gt;

mapred.tasktracker.map.tasks.maximum

&lt;/name&gt;


> > 

&lt;value&gt;

4

&lt;/value&gt;



> 

&lt;/property&gt;


> > 

&lt;property&gt;


> > > 

&lt;name&gt;

mapred.tasktracker.reduce.tasks.maximum

&lt;/name&gt;


> > > 

&lt;value&gt;

4

&lt;/value&gt;



> > 

&lt;/property&gt;


> > 

&lt;property&gt;


> > > 

&lt;name&gt;

mapred.job.tracker.info.port

&lt;/name&gt;


> > > 

&lt;value&gt;

47040

&lt;/value&gt;



> > 

&lt;/property&gt;


> > 

&lt;property&gt;


> > > 

&lt;name&gt;

mapred.task.tracker.output.port

&lt;/name&gt;


> > > 

&lt;value&gt;

47041

&lt;/value&gt;



> > 

&lt;/property&gt;


> > 

&lt;property&gt;


> > > 

&lt;name&gt;

mapred.task.tracker.report.port

&lt;/name&gt;


> > > 

&lt;value&gt;

47042

&lt;/value&gt;



> > 

&lt;/property&gt;


> > 

&lt;property&gt;


> > > 

&lt;name&gt;

mapred.job.tracker.info.bindAddress

&lt;/name&gt;


> > > 

&lt;value&gt;

10.145.12.136

&lt;/value&gt;



> > 

&lt;/property&gt;


> > 

&lt;property&gt;


> > > 

&lt;name&gt;

tasktracker.http.port

&lt;/name&gt;


> > > 

&lt;value&gt;

40069

&lt;/value&gt;



> > 

&lt;/property&gt;


> > 

&lt;property&gt;


> > > 

&lt;name&gt;

mapred.job.reuse.jvm.num.tasks

&lt;/name&gt;


> > > 

&lt;value&gt;

2

&lt;/value&gt;



> > 

&lt;/property&gt;


> > 

&lt;property&gt;


> > > 

&lt;name&gt;

mapred.min.split.size

&lt;/name&gt;


> > > 

&lt;value&gt;

268435456

&lt;/value&gt;



> > 

&lt;/property&gt;


> > 

&lt;property&gt;


> > > 

&lt;name&gt;

mapred.map.tasks.speculative.execution

&lt;/name&gt;


> > > 

&lt;value&gt;

false

&lt;/value&gt;



> > 

&lt;/property&gt;


> > 

&lt;property&gt;


> > > 

&lt;name&gt;

mapred.reduce.tasks.speculative.execution

&lt;/name&gt;


> > > 

&lt;value&gt;

false

&lt;/value&gt;



> > 

&lt;/property&gt;


> > 

&lt;property&gt;


> > > 

&lt;name&gt;

mapred.map.child.java.opts

&lt;/name&gt;


> > > 

&lt;value&gt;

-Xmx2048M

&lt;/value&gt;



> > 

&lt;/property&gt;


> > 

&lt;property&gt;


> > > 

&lt;name&gt;

mapred.reduce.child.java.opts

&lt;/name&gt;


> > > 

&lt;value&gt;

-Xmx2048M

&lt;/value&gt;



> 

&lt;/property&gt;




&lt;/configuration&gt;

  }}}```