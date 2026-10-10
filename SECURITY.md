# Security policy

mizuchi is a query engine library that may be embedded by runners that execute
untrusted SQL. A way for a query to read files or data it was not given,
exceed configured memory limits without failing, crash the host process from
crafted input, or corrupt another query's results is a security vulnerability.

## Reporting a vulnerability

Report vulnerabilities privately through GitHub private vulnerability
reporting: <https://github.com/Sannrox/mizuchi/security/advisories/new>.
Do not open a public issue, pull request, or discussion.

Please include the mizuchi version, the query or input that triggers it, and a
minimal reproduction.

## Supported versions

mizuchi has no releases yet. Once it does, only the latest release receives
security fixes.
