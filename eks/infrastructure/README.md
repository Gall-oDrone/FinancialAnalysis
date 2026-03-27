# Financial Analysis - EKS Infrastructure

This directory contains Infrastructure as Code (IaC) for deploying the Financial Analysis application to Amazon EKS.

## Directory Structure

```
eks/infrastructure/
├── cloudformation/           # AWS CloudFormation templates
│   ├── vpc/                  # VPC and networking
│   ├── eks/                  # EKS cluster configuration
│   ├── rds/                  # RDS PostgreSQL database
│   └── monitoring/           # CloudWatch and monitoring
├── terraform/                # Terraform configurations
│   ├── modules/              # Reusable Terraform modules
│   │   ├── vpc/
│   │   ├── eks/
│   │   ├── rds/
│   │   └── security/
│   ├── environments/         # Environment-specific configs
│   │   ├── dev/
│   │   ├── staging/
│   │   └── prod/
│   └── global/               # Shared/global resources
├── kubernetes/               # Kubernetes manifests
│   ├── base/                 # Base configurations
│   ├── overlays/             # Environment overlays
│   └── helm/                 # Helm charts
└── scripts/                  # Deployment scripts
```

## Prerequisites

- AWS CLI v2 configured with appropriate credentials
- Terraform >= 1.5.0
- kubectl >= 1.28
- Helm >= 3.12
- eksctl (optional, for EKS management)

## Quick Start

### Using CloudFormation

```bash
# Deploy VPC
aws cloudformation deploy \
  --template-file cloudformation/vpc/vpc-stack.yaml \
  --stack-name financial-analysis-vpc \
  --parameter-overrides Environment=dev

# Deploy EKS Cluster
aws cloudformation deploy \
  --template-file cloudformation/eks/eks-cluster.yaml \
  --stack-name financial-analysis-eks \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --parameter-overrides Environment=dev
```

### Using Terraform

```bash
cd terraform/environments/dev

# Initialize Terraform
terraform init

# Plan deployment
terraform plan -out=tfplan

# Apply deployment
terraform apply tfplan
```

### Configure kubectl

```bash
aws eks update-kubeconfig --name financial-analysis-dev --region us-east-1
```

## Architecture

The infrastructure includes:

- **VPC**: Multi-AZ VPC with public/private subnets
- **EKS Cluster**: Managed Kubernetes cluster
- **Node Groups**: Auto-scaling worker nodes
- **RDS PostgreSQL**: Managed database for financial data
- **ECR**: Container registry for application images
- **ALB Ingress**: Application Load Balancer for ingress
- **Secrets Manager**: Secure credential storage
- **CloudWatch**: Monitoring and logging

## Security Features

- Private EKS API endpoint (optional public access)
- Pod security policies
- Network policies
- IAM roles for service accounts (IRSA)
- Secrets encryption with AWS KMS
- Security groups with minimal required access

## Cost Optimization

- Spot instances for development workloads
- Right-sized node groups
- Auto-scaling policies
- Reserved capacity for production

## Monitoring

- CloudWatch Container Insights
- Prometheus metrics (optional)
- Grafana dashboards (optional)
- AWS X-Ray for distributed tracing

## License

See the main project LICENSE file.

