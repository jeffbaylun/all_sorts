#
# Data sources
#
data "aws_eks_cluster" "this" {
  name = var.eks_cluster_name
}
data "aws_iam_openid_connect_provider" "this" {
  url = data.aws_eks_cluster.this.identity[0].oidc[0].issuer
}
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

#
# Local variables
#
locals {
  aws_iam_role_creation = (var.aws_iam_role_arn != null || local.enabled_feeds == {}) ? 0 : 1
  aws_iam_role_arn      = coalesce(
    var.aws_iam_role_arn,
    length(module.aws_iam_role) > 0 ? try(module.aws_iam_role[0].iam_role_arn, null) : null
  )
  aws_iam_role_name     = coalesce(
    try(split("/", var.aws_iam_role_arn)[length(split("/", var.aws_iam_role_arn)) - 1], null),
    try(module.aws_iam_role[0].iam_role_name, null)
  )

  target_table_s3_arns  = distinct(
      [
        for glue_table in data.aws_glue_catalog_table.this : 
        "arn:aws:s3:::${replace(glue_table.storage_descriptor[0].location, "s3://", "")}/*"
      ]
  )
  target_table_glue_arns = distinct(
      [
        for glue_table in data.aws_glue_catalog_table.this : 
        "${join(":", slice(split(":", glue_table.arn), 0, 4))}:${glue_table.catalog_id}:${join(":", slice(split(":", glue_table.arn), 5, length(split(":", glue_table.arn))))}"
      ]
  )
  ingest_queues_arns    = distinct(concat(
    [
      for staging in module.ingest_enabled_feed_by_s3_location_aws_sqs : staging.ingest_queues_arn
    ],
    [
      for v in local.enabled_feeds :
        format(
            "arn:aws:sqs:%s:%s:%s",
            split(".", replace(v.app_config["TITANFLOW_DF_REGISTRATION_SVC_INGEST_SQS_QUEUE_URLS"], "https://", "")) [1],   # region
            split("/", v.app_config["TITANFLOW_DF_REGISTRATION_SVC_INGEST_SQS_QUEUE_URLS"]) [3],                            # account ID
            split("/", v.app_config["TITANFLOW_DF_REGISTRATION_SVC_INGEST_SQS_QUEUE_URLS"]) [4]                             # queue name    
        )
      if contains(keys(v.app_config), "TITANFLOW_DF_REGISTRATION_SVC_INGEST_SQS_QUEUE_URLS")
    ]
  ))

  # S3: bucket + (optional) first folder, depth = 2
  s3_prefix_depth2 = try(
    distinct([
      for arn in local.target_table_s3_arns :
      regexall("^arn:aws:s3:::[^/]+(?:/[^/]+)?", arn)[0]
    ]),
    []
  )
  grouped_s3_prefixes = sort([
    for p in local.s3_prefix_depth2 :
    "${p}/*"
  ])
  # SQS: full queue name, depth = 1
  sqs_prefix = try(
    distinct([
      for arn in local.ingest_queues_arns :
      regexall("^arn:aws:sqs:[^:]+:[^:]+:[^/]+", arn)[0]
    ]),
    []
  )
  grouped_sqs_prefixes = sort(local.sqs_prefix)
  # Glue: database/table grouping, depth = 3
  glue_prefix_depth3 = try(
    distinct([
      for arn in local.target_table_glue_arns :
      regexall("^arn:aws:glue:[^:]+:[^:]+:table/[^/]+/[^/]+", arn)[0]
    ]),
    []
  )
  grouped_glue_prefixes = sort([
    for p in local.glue_prefix_depth3 :
    "${p}/*"
  ])
}

#
# Resources
#
module "aws_iam_role" {
  count = local.aws_iam_role_creation
  source = "git::https://github.com/comcast-zorrillo/titanflow-infra//modules/aws/iam-role?ref=modules/aws/iam-role/v0.2.0"

  iam_role_suffix    = "${local.name}-iceberg-df-reg-svc-eks-pod-role"
  assume_role_policy = jsonencode({
    version = "2012-10-17"
    statement = [
      {
        effect        = "Allow"
        action        = "sts:AssumeRoleWithWebIdentity"
        principal     = {federated = data.aws_iam_openid_connect_provider.this.arn}
      },

      {
        effect        = "Allow"
        action        = ["sts:AssumeRole"]
        principal     = {service = "ec2.amazonaws.com"}
      },

      {
        sid           = "AllowApplicationOwnersToAssumeRole"
        effect        = "Allow"
        action        = ["sts:AssumeRole"]
        principal     = {aws = "arn:aws:iam::${data.aws_caller_identity.this.account_id}:role/ApplicationOwner"}
      }
    ]
  })
}

data "aws_iam_policy_document" "queue_consumer_access" {
  version = "2012-10-17"

  statement {
    effect    = "Allow"
    actions   = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:GetQueueUrl",
      "sqs:ChangeMessageVisibility"
    ]

    resources = local.grouped_sqs_prefixes
  }
}

resource "aws_iam_role_policy" "queue_consumer_access" {
  name   = "queue-consumer-access-${local.module_id}"
  role   = local.aws_iam_role_name
  policy = data.aws_iam_policy_document.queue_consumer_access.json
}

data "aws_iam_policy_document" "s3_write_access" {
  version   = "2012-10-17"

  statement {
    sid     = "PutMetadataAndDatafiles"
    effect  = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetObject",
      "s3:GetObjectTagging",
      "s3:PutObject",
      "s3:PutObjectTagging",
    ]

    resources = local.grouped_s3_prefixes
  }
}

resource "aws_iam_role_policy" "s3_write_access" {
  name   = "s3-write-access-${local.module_id}"
  role   = local.aws_iam_role_name
  policy = data.aws_iam_policy_document.s3_write_access.json
}

data "aws_iam_policy_document" "glue_write_access" {
  version   = "2012-10-17"

  statement {
    sid     = "GetCatalog"
    effect  = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:UpdateTable"
    ]

    resources = local.grouped_glue_prefixes
  }
}

resource "aws_iam_role_policy" "glue_write_access" {
  name   = "glue-write-access-${local.module_id}"
  role   = local.aws_iam_role_name
  policy = data.aws_iam_policy_document.glue_write_access.json
}

data "aws_iam_policy_document" "sideline_access" {
  version   = "2012-10-17"
  statement {
    effect  = "Allow"
    actions = [
      "sqs:SendMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:GetQueueUrl",
      "sqs:ListQueues"
    ]

    resources = length(local.ingest_queues_arns) > 32 ? local.ingest_queues_arns : ["*"]
  }
}

resource "aws_iam_role_policy" "sideline_access" {
  name   = "sideline-access-${local.module_id}"
  role   = local.aws_iam_role_name
  policy = data.aws_iam_policy_document.sideline_access.json
}

resource "kubernetes_service_account" "this" {
  metadata {
      name      = local.service_name
      namespace = var.kubernetes_namespace
      annotations = {
        "eks.amazonaws.com/role-arn" = local.aws_iam_role_arn
      }
  }
}