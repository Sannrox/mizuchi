# Cluster deployment

mizuchi's coordinator is an in-process worker pool. The engine never opens
network connections; a runner maps worker slots onto processes or pods.

- Standalone: `deploy/compose.yaml` starts a coordinator container and a
  worker container sharing a spill volume.
- Kubernetes: `deploy/kubernetes.yaml` is a Deployment pair a continuous
  delivery tool can apply.
- Dynamic allocation and speculative copies are `Cluster` APIs. See
  `ClusterConfig`.
