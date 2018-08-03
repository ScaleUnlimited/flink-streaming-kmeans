# Background

[K-Means](https://en.wikipedia.org/wiki/K-means_clustering) is a simple algorithm for clustering data. There are many improvements, including [K-Means++](https://en.wikipedia.org/wiki/K-means%2B%2B), [Scalable K-Means++](http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf), etc. This is a simple version that assumes (a) some set of candidate cluster centroids to start with, and (b) a continuous stream of features that will be clustered.

# Theory

As we process each feature, we calculate the distance to the closest cluster centroid. The feature is assigned to this centroid, which shifts it towards the feature, since the centroid is the average of all features assigned to it. If the feature was previously assigned to a different cluster, it is removed from that cluster's centroid before adding to the new cluster's centroid.

Features are repeatedly processed until they are "stable", which we define as not having moved between clusters for N iterations.

# Flink Implementation

We broadcast the initial centroids and the deltas (feature addition and deletion) to all ClusterFunction operators. Features are shuffled (randomly partitioned) between ClusterFunction operators.

The side outputs of the ClusterFunction are updates to Centroids, and features that aren't yet stable. These close out the centroid and feature iteration streams. The regular output from the ClusterFunction is a stable Feature, plus the current Centroid it's assigned to.
