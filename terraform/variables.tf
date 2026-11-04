variable "aws_region" {
  description = "AWS deployment region"
  type        = string
}

variable "project_name" {
  description = "Project identifier"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "vpc_cidr" {
  description = "VPC IPv4 CIDR"
  type        = string
}

variable "availability_zones" {
  description = "Availability zones"
  type        = list(string)
}

variable "db_instance_class" {
  description = "RDS PostgreSQL instance class"
  type        = string
}

variable "db_engine_version" {
  description = "PostgreSQL engine version"
  type        = string
}

variable "db_name" {
  description = "Initial PostgreSQL database name"
  type        = string
}

variable "db_username" {
  description = "RDS master username"
  type        = string
}

variable "db_allocated_storage" {
  description = "Initial RDS storage in GB"
  type        = number
}

variable "backup_retention_period" {
  description = "Automated backup retention in days"
  type        = number
}

variable "bastion_ssh_cidr" {
  description = "Public IP allowed to SSH into the bastion"
  type        = string
}