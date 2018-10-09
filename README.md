# Background

[K-Means](https://en.wikipedia.org/wiki/K-means_clustering) is a simple algorithm for clustering data. There are many improvements, including [K-Means++](https://en.wikipedia.org/wiki/K-means%2B%2B), [Scalable K-Means++](http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf), etc. This is a simple version that clusters a continuous stream of features, using a "max distance" to trigger the creation of a new cluster (up to a predefined limit).

# Theory

As we process each feature, we calculate the distance to the closest cluster centroid. The feature is assigned to this cluster, which shifts the cluster towards the feature, since the centroid is the average of all features assigned to it. If the feature was previously assigned to a different cluster, it is removed from that cluster's centroid before adding to the new cluster's centroid.

Features are repeatedly processed until they are "stable", which we define as not having moved between clusters for N iterations. When a feature is stable, it is output as a "cluster result", and also removed from the cluster that it was assigned to; this ensures that clusters will "evolve" over time, as only newer features influence the centroid.

# Flink Implementation

We broadcast the cluster updates (feature addition and deletion) to all ClusterFunction operators. Features are partitioned by their id between ClusterFunction operators.

The side outputs of the ClusterFunction are updates to clusters, and features that aren't yet stable. These close out the cluster and feature iteration streams. The regular output from the ClusterFunction is a stable Feature, plus information about the cluster it's assigned to (cluster id and current centroid).

# KMeansTool

To run the tool from Eclipse, set up `com.scaleunlimited.flinkkmeans.KMeansTool` as the main class, with the following parameters:

- `-local` (to specify running Flink locally, versus on a real cluster)
- `-input <path to input file>` (e.g. `/path/to/flink-streaming-kmeans/src/test/resources/citibike-20180801-min.tsv`)
- `-accesstoken <MapBox access token>`
- `-clusters <number of cluster>` (5 or 10 are good values)
- `-queryable` (to enable calls to the API, on port 8085).

Once the tool is running with the above options, then open `http://localhost:8085/map` in your browser.
