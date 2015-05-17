SimpleDynamo
=======

SimpleDynamo is an Android app that implements a simplified version of Dynamo. It includes partitioning, replication and failure handling.
The main goal of this project was to provide both availability and linearizability at the same time. Like the SimpleDHT app, it supports insert, query and delete requests in a distributed fashion based on Dynamo and recognizes the two special strings for the selection parameter in query and delete operations - "*" and "@".

This app detects and handles node failures as well as node recoveries, while maintaining dynamic replication consistency. Replication is implemented as per Dynamo. In other words, a <key, value> pair is replicated over three consecutive partitions, starting from the partition that the key belongs to. The SHA-1 hash function was used to generate partitioning key.

Unlike Dynamo, virtual nodes and hinted handoff are not implemented for this project. This means that the app uses physical rather than virtual nodes and all partitions are static and fixed. When there is a failure, the replication is done only on two nodes. The app supports concurrent read/write operations and can handle failure occurring at the same time. The app also implemented per-key linearizability and consistency. This means that all replicas store the same value for each key.