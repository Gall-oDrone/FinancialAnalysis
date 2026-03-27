# =============================================================================
# Helm add-ons (metrics-server, ALB controller, ExternalDNS, cert-manager,
# external-secrets) - aligned with microservices-trading-bot infrastructure.
# =============================================================================

resource "helm_release" "metrics_server" {
  name       = "metrics-server"
  repository = "https://kubernetes-sigs.github.io/metrics-server/"
  chart      = "metrics-server"
  namespace  = "kube-system"
  version    = "3.12.2"

  depends_on = [module.eks, module.iam_irsa, module.security]
}

resource "helm_release" "aws_load_balancer_controller" {
  name       = "aws-load-balancer-controller"
  repository = "https://aws.github.io/eks-charts"
  chart      = "aws-load-balancer-controller"
  namespace  = "kube-system"
  version    = "1.8.2"

  set {
    name  = "clusterName"
    value = module.eks.cluster_name
  }

  set {
    name  = "region"
    value = var.aws_region
  }

  set {
    name  = "vpcId"
    value = module.vpc.vpc_id
  }

  set {
    name  = "serviceAccount.create"
    value = true
  }

  values = [
    yamlencode({
      serviceAccount = {
        annotations = {
          "eks.amazonaws.com/role-arn" = module.iam_irsa.irsa_role_arns["alb"]
        }
      }
    })
  ]

  depends_on = [module.eks, module.iam_irsa, module.security]
}

resource "time_sleep" "wait_for_alb_controller_webhook" {
  depends_on      = [helm_release.aws_load_balancer_controller]
  create_duration = "90s"
}

resource "helm_release" "external_dns" {
  name       = "external-dns"
  repository = "https://kubernetes-sigs.github.io/external-dns/"
  chart      = "external-dns"
  namespace  = "kube-system"
  version    = "1.15.0"

  set {
    name  = "provider"
    value = "aws"
  }

  set {
    name  = "policy"
    value = "upsert-only"
  }

  set {
    name  = "registry"
    value = "txt"
  }

  set {
    name  = "txtOwnerId"
    value = local.name
  }

  set {
    name  = "serviceAccount.create"
    value = true
  }

  values = [
    yamlencode({
      serviceAccount = {
        annotations = {
          "eks.amazonaws.com/role-arn" = module.iam_irsa.irsa_role_arns["external-dns"]
        }
      }
    })
  ]

  depends_on = [module.eks, module.iam_irsa, module.security]
}

resource "helm_release" "cert_manager" {
  name       = "cert-manager"
  repository = "https://charts.jetstack.io"
  chart      = "cert-manager"
  namespace  = "cert-manager"
  version    = "v1.15.1"

  create_namespace = true

  depends_on = [time_sleep.wait_for_alb_controller_webhook]

  set {
    name  = "installCRDs"
    value = true
  }

  set {
    name  = "serviceAccount.create"
    value = true
  }

  values = [
    yamlencode({
      serviceAccount = {
        annotations = {
          "eks.amazonaws.com/role-arn" = module.iam_irsa.irsa_role_arns["cert-manager"]
        }
      }
    })
  ]
}

resource "helm_release" "external_secrets" {
  name       = "external-secrets"
  repository = "https://charts.external-secrets.io"
  chart      = "external-secrets"
  namespace  = "external-secrets"
  version    = "0.9.14"

  create_namespace = true

  depends_on = [time_sleep.wait_for_alb_controller_webhook]

  set {
    name  = "serviceAccount.create"
    value = true
  }

  values = [
    yamlencode({
      serviceAccount = {
        annotations = {
          "eks.amazonaws.com/role-arn" = module.iam_irsa.irsa_role_arns["external-secrets"]
        }
      }
    })
  ]
}
