# Security Notes for Public Release

## What is sanitized in this repository
- No hardcoded credentials in source code.
- No internal Oracle DSN defaults in configuration.
- No internal network share paths in notebooks.

## Local configuration
Use environment variables (see `.env.example`):
- `ORACLE_USER`
- `ORACLE_PASSWORD`
- `ORACLE_DSN`
