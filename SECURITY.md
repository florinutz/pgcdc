# Security Policy

## Reporting Vulnerabilities

If you discover a security vulnerability, please report it responsibly:

- **GitHub**: Use [Security Advisories](https://github.com/florinutz/pgcdc/security/advisories/new)

Do not open public issues for security vulnerabilities.

## Supported Versions

| Version | Supported |
|---------|-----------|
| latest  | Yes       |

## Security Features

- **HMAC-SHA256 webhook signing** (`--signing-key`): Verify event authenticity
- **TLS** for SSE, metrics, and serve endpoints (`--tls-cert`, `--tls-key`)
- **SQL injection prevention**: All identifiers sanitized via `pgx.Identifier{}.Sanitize()`
- **Credential isolation**: Passwords accepted via env vars (`PGCDC_DATABASE_URL`) — avoid CLI flags in production
