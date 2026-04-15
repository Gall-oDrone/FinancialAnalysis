# Apache Airflow (Development)

This folder contains Helm values used by the GitHub Actions deployment workflow
to install or upgrade Apache Airflow in the development EKS cluster.

## Files

- `values-development.yaml`: baseline Airflow configuration with:
  - webserver, scheduler, and worker replicas
  - ALB ingress configuration
  - DAG git-sync settings

## Notes

- Replace the default admin credentials before using in shared environments.
- Update `ingress.web.hosts` with a real DNS name for your environment.
- If DAGs come from a private repository, configure git-sync credentials
  (for example, via Kubernetes Secret and chart values).
