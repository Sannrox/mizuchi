# Cluster deployment

Coordinator and worker processes talk over Arrow Flight. TCP is opened
only when the embedder sets `MIZUCHI_ALLOW_NETWORK` (ADR 0008, ADR 0011).

- Standalone: `deploy/compose.yaml` starts three worker containers and a
  coordinator. Shuffle files share the `/spill` volume; a reader that
  cannot fetch a Flight stream falls back to that file.
- Kubernetes: `deploy/kubernetes.yaml` is a Deployment pair a continuous
  delivery tool can apply.
- The optional CI job `.github/workflows/flight.yml` (`workflow_dispatch`
  only) runs the three-worker suite, including a mid-query kill that
  still returns a correct result.
- In-process worker slots remain `Cluster`. Flight workers are
  `RemoteCluster` behind `--features flight`.
