# Security policy

Report vulnerabilities through the repository's
[private reporting channel](https://github.com/faustbrian/go-rabbitmq-queues/security/advisories/new).
Do not include production credentials, certificates,
private keys, message payloads, application headers, routing identities,
broker topology, customer data, or unsanitized broker errors in a public issue.

The module owns verified TLS configuration, bounded connection-attempt contexts,
secret-safe public errors and observations, hostile-input bounds, and best-
effort zeroization of password snapshots returned for connection attempts. It
does not protect a compromised process or broker, provider-retained password
aliases, application logging of payloads, incorrect broker authorization, or
credentials exposed outside the package boundary.

Credential providers and handlers are trusted application collaborators. They
must honor their contexts; the module cannot forcibly stop arbitrary caller
code that ignores cancellation. Native protocol decoding happens before wrapper
delivery admission, so broker ingress policies remain a deployment obligation.

The [versioned threat model](docs/security-threat-model.md) records the owned
controls and qualification boundaries. Severity, response targets, remediation,
embargo and coordinated publication follow the shared
[vulnerability management process](https://github.com/faustbrian/go-library-tools/blob/main/docs/ecosystem/security/vulnerability-management.md).
Published versions and unreleased main are distinct; a source fix does not
establish a qualified public security release or downstream upgrade.
