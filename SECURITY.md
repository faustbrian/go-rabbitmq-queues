# Security policy

Report vulnerabilities through the repository's private vulnerability
reporting channel. Do not include production credentials, certificates,
private keys, message payloads, application headers, routing identities,
broker topology, customer data, or unsanitized broker errors in a public issue.

The module owns verified TLS configuration, bounded credential-provider calls,
secret-safe public errors and observations, hostile-input bounds, and best-
effort zeroization of password snapshots returned for connection attempts. It
does not protect a compromised process or broker, provider-retained password
aliases, application logging of payloads, incorrect broker authorization, or
credentials exposed outside the package boundary.
