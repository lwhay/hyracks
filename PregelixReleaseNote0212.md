Feature Notes: This is the fifth release of Pregelix, which supports a major subset of the standard Pregelix API, including:

-- Vertex API

-- Send/receive messages

-- Message combiner (Optional)

-- Global Aggregator (Optional)

-- Graph Mutations

-- Different graph storage options: standard B-trees and LSM B-trees.

New Features

## New features ##
New features in this release:
| **Vertex overflow support** | A vertex can have arbitrarily large byte content, while in previous releases that number is<br /> bounded by the frame size. |
|:----------------------------|:-----------------------------------------------------------------------------------------------------------------------------|
| **Job pipelining** |  Two contingent Pregelix jobs can be pipelined if user set the job pipelining option and the vertex format<br /> in the two jobs are compatible.|
| **Performance profiling** | Pregelix reports to a client the IO counters, network counters, and memory counters for each executed job. |
| **Performance improvement** | We improved the performance for both long-running jobs and quick iterations. <br />Check out our performance benchmark: http://pregelix.ics.uci.edu/performance.html|