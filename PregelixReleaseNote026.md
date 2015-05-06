### Feature Notes ###
This is the third release of Pregelix, which supports a major subset of the standard Pregelix API, including:

-- Vertex API

-- Send/receive messages

-- Message combiner (Optional)

-- Global Aggregator (Optional)

-- Normalized Key Computer (Optional)

-- Graph Mutations

### New Features ###
| **Cache-sensitive optimization** | Pregelix jobs can set an optional normalized key computers to get the standard [Alpha-sort](https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=4&cad=rja&ved=0CEkQFjAD&url=http%3A%2F%2Fresearch.microsoft.com%2F~gray%2FAlphaSort.doc&ei=5ASxUc_dLuOGjAKk1YH4Bg&usg=AFQjCNEh_ZXt0Nqcv7ghuzaibSj8Vw9o9g&sig2=EQOp-ImJbHHUl7IE9tTZKg&bvm=bv.47534661,d.cGE) optimization. <br /> This will dramatically improve the message grouping performance. |
|:---------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Improved job lifecycle management** | The job deployment time is significantly reduced. |
| **Binary distribution** | Users do not need to build the source code from scratch, instead, we offer distribution binaries directly. |