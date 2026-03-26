# Security Notes for Public Release

## What is sanitized in this repository
- No hardcoded credentials in source code.
- No internal Oracle DSN defaults in configuration.
- No internal network share paths in notebooks.
- No client-specific schema names in SQL examples.

## Local configuration
Use environment variables (see `.env.example`):
- `ORACLE_USER`
- `ORACLE_PASSWORD`
- `ORACLE_DSN`

## Required before making this repository public
1. Rotate any credentials that were ever committed in git history.
2. Rewrite git history to remove leaked secrets from old commits.
3. Verify with a secret scanner (`gitleaks` or `trufflehog`) before publishing.
