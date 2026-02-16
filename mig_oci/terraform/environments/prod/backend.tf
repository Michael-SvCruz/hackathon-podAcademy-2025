terraform {
  # Fase 0-2: State local (para iniciar rápido)
  backend "local" {
    path = "terraform.tfstate"
  }

  # Fase 3+: Migrar para OCI Object Storage (após buckets criados)
  # Descomentar e executar: terraform/scripts/migrate_state_to_remote.sh
  #
  # backend "s3" {
  #   bucket                      = "tfstate-hackathon-2025"
  #   key                         = "prod/terraform.tfstate"
  #   region                      = "sa-saopaulo-1"
  #   endpoint                    = "https://<namespace>.compat.objectstorage.sa-saopaulo-1.oraclecloud.com"
  #   skip_region_validation      = true
  #   skip_credentials_validation = true
  #   skip_metadata_api_check     = true
  #   force_path_style            = true
  # }
}
