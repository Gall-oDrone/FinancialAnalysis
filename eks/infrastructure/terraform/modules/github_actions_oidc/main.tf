# OIDC for GitHub Actions → IAM role for ECR + EKS (same pattern as AWS docs:
# https://docs.github.com/en/actions/deployment/security-hardening-your-deployments/configuring-openid-connect-in-amazon-web-services

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name

  github_oidc_url = "https://token.actions.githubusercontent.com"
  # Subject claim: repo:ORG/REPO:ref:refs/heads/branch or repo:ORG/REPO:environment:name
  repo_subject = "repo:${var.github_organization}/${var.github_repository}:*"

  oidc_provider_arn = var.create_oidc_provider ? aws_iam_openid_connect_provider.github[0].arn : var.oidc_provider_arn
}

resource "aws_iam_openid_connect_provider" "github" {
  count = var.create_oidc_provider ? 1 : 0

  url = local.github_oidc_url

  client_id_list = [
    "sts.amazonaws.com",
  ]

  # TLS SHA-1 thumbprint of token.actions.githubusercontent.com (40 hex chars).
  # If GitHub rotates certs, fetch: echo | openssl s_client -servername token.actions.githubusercontent.com -connect token.actions.githubusercontent.com:443 2>/dev/null | openssl x509 -outform DER | openssl sha1 -r
  thumbprint_list = [
    "7560d6f40fa55195f740ee2b1b7c0b4836cbe103",
  ]

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-github-oidc"
  })
}

data "aws_iam_policy_document" "github_actions_assume" {
  statement {
    sid     = "AllowGitHubActionsOIDC"
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = [local.repo_subject]
    }
  }
}

resource "aws_iam_role" "github_actions" {
  name                 = "${var.name_prefix}-github-actions"
  description          = "GitHub Actions CI for ${var.github_organization}/${var.github_repository} (ECR push, EKS deploy)"
  assume_role_policy   = data.aws_iam_policy_document.github_actions_assume.json
  max_session_duration = 3600

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-github-actions"
  })
}

data "aws_iam_policy_document" "ecr_push" {
  statement {
    sid    = "EcrAuth"
    effect = "Allow"
    actions = [
      "ecr:GetAuthorizationToken",
    ]
    resources = ["*"]
  }

  dynamic "statement" {
    for_each = var.ecr_repository_names
    content {
      sid    = "EcrPush-${replace(statement.value, "/", "-")}"
      effect = "Allow"
      actions = [
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "ecr:PutImage",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload",
        "ecr:DescribeRepositories",
        "ecr:CreateRepository",
      ]
      resources = [
        "arn:aws:ecr:${local.region}:${local.account_id}:repository/${statement.value}",
      ]
    }
  }
}

resource "aws_iam_role_policy" "ecr_push" {
  name   = "ecr-push"
  role   = aws_iam_role.github_actions.id
  policy = data.aws_iam_policy_document.ecr_push.json
}

data "aws_iam_policy_document" "eks_read" {
  count = var.eks_cluster_name != "" ? 1 : 0

  statement {
    sid    = "EksDescribe"
    effect = "Allow"
    actions = [
      "eks:DescribeCluster",
    ]
    resources = [
      "arn:aws:eks:${local.region}:${local.account_id}:cluster/${var.eks_cluster_name}",
    ]
  }

  statement {
    sid       = "EksList"
    effect    = "Allow"
    actions   = ["eks:ListClusters"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "eks_read" {
  count = var.eks_cluster_name != "" ? 1 : 0

  name   = "eks-describe"
  role   = aws_iam_role.github_actions.id
  policy = data.aws_iam_policy_document.eks_read[0].json
}

resource "aws_eks_access_entry" "github_actions" {
  count = var.enable_eks_access_entry && var.eks_cluster_name != "" ? 1 : 0

  cluster_name      = var.eks_cluster_name
  principal_arn     = aws_iam_role.github_actions.arn
  kubernetes_groups = []
  type              = "STANDARD"

  depends_on = [aws_iam_role.github_actions]
}

resource "aws_eks_access_policy_association" "github_actions" {
  count = var.enable_eks_access_entry && var.eks_cluster_name != "" ? 1 : 0

  cluster_name  = var.eks_cluster_name
  principal_arn = aws_iam_role.github_actions.arn
  policy_arn    = var.eks_access_policy_arn

  access_scope {
    type = "cluster"
  }

  depends_on = [aws_eks_access_entry.github_actions]
}
