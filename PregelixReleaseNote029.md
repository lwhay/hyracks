Feature Notes
This is the third release of Pregelix, which supports a major subset of the standard Pregelix API, including:

-- Vertex API

-- Send/receive messages

-- Message combiner (Optional)

-- Global Aggregator (Optional)

-- Normalized Key Computer (Optional)

-- Graph Mutations

-- Different graph storage options: standard B-trees and LSM B-trees.

New Features
| **LSM storage option** | Pregelix job can specify a LSM option to let the underlying storage be LSM B-trees instead of standard B-trees,  such<br /> that updates are faster though search will be slightly slower than the standard B-Tree. |
|:-----------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Message overflow support** | A vertex can receive an arbitrary number of uncombined messages, while in previous releases that number is<br /> bounded by the frame size. |
| **Partition early termination**| In a superstep,  a vertex can choose to terminate the computation of its current partition, such that all the<br /> consequent vertices to be processed in the same partition will be skipped and vote to halt. |