# Changelog

All notable changes to this project will be documented in this file.

## 1.1.0 - 2025-01-24

### Added
- **Backup Mode**: New `backup: true` flag for destinations to store compressed archives instead of individual files
- **Catalog System**: Automated S3 catalog with JSONL metadata files for backup content tracking
- **Athena Integration**: AWS Glue crawler, database, and Athena workgroup for querying backup contents
- **Mixed Mode Support**: Ability to have both backup and normal destinations for the same source
- **Comprehensive Documentation**: New [BACKUP_MODE.md](docs/BACKUP_MODE.md) with complete backup mode guide

### Changed
- **Target Container**: Modified to handle backup destinations and write catalog metadata

### Technical Notes
- **Container Rebuild Required**: Run `./build_containers.sh` before deploying
- **Infrastructure Changes**: New Glue and Athena resources created automatically
- **Backward Compatible**: Existing configurations continue to work without changes

## 1.0.0 - 2025-05-01

:seedling: Initial release.