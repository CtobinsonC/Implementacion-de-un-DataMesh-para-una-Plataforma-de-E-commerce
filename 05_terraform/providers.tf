terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.5"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

variable "project_id" {
  description = "ID del proyecto GCP"
  type        = string
  default     = "gcp-implementacion-datamesh"
}

variable "region" {
  description = "Región por defecto"
  type        = string
  default     = "us-central1"
}
