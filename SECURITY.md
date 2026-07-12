# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in Incorporator, **please do not
open a public GitHub issue.** Public issues are scraped by automated
exploit pipelines almost immediately.

Instead, report it privately via one of these channels:

1. **GitHub private vulnerability reports** (preferred):
   <https://github.com/PyPlumber/incorporator/security/advisories/new>
2. **Email**: open a placeholder GitHub issue without details and ask a
   maintainer to follow up by email, or use the GitHub security tab.

We aim to acknowledge reports within **72 hours** and to ship a patched
release within **14 days** for high-severity issues. Coordinated
disclosure timelines can be negotiated.

## Supported Versions

Security fixes are issued against the latest minor release. Older
versions are not patched — please upgrade.

| Version | Supported |
|---------|-----------|
| 1.4.x   | ✅        |
| 1.3.x   | ❌        |
| 1.2.x   | ❌        |
| 1.1.x   | ❌        |
| 1.0.x   | ❌        |
| < 1.0   | ❌        |

## Security Posture

Incorporator is a client-side framework — most of its attack surface is
the parsing layer rather than network-facing services. The framework
hardens against the following classes of vulnerability by default:

### XML — XXE & Billion Laughs
The XML handler runs a centralised `check_xml_security` pass on every
payload *before* either `lxml` or stdlib `xml.etree` sees it. External
entity resolution is disabled, network fetches inside the parser are
blocked, and entity-expansion bombs are rejected with a clear error
rather than silently consumed. lxml is preferred when installed; stdlib
ElementTree (which has no built-in XXE protection) is hardened by the
pre-parse check.

### URL Scheme Validation
HTTP requests reject any scheme other than `http://` or `https://` —
no `file://`, `ftp://`, `gopher://`, or scheme confusion attacks. The
check runs in `_validate_url` before any network I/O.

### Compression Bombs
The framework decompresses payloads in a background thread with bounded
buffering. Pathological compression ratios trip the format parser's
streaming guards rather than blowing out memory.

### Archive Path Traversal
ZIP and TAR member names are validated against absolute paths, drive
prefixes, and `..` traversal before any extraction, and Apple
resource-fork junk (`__MACOSX`) is filtered out. Extraction selects the
single member matching the requested data format; an archive containing
more than one matching file is rejected unless you name one explicitly
via `archive_target`. No arbitrary path writes outside the archive root.

### Secret Handling
- Pipeline configs reference secrets by name (`${API_KEY}`,
  `${file:/run/secrets/api_key}`) — secrets never live in the JSON.
- `failed_sources` URLs are redacted before they hit the structured log
  files. Query-string credentials (`?api_key=…&token=…`) are scrubbed.
- `pipeline.json` and `.env` are gitignored by default.

### Out of Scope

The following are *not* part of the security boundary and rely on the
deploying operator:

- **Authentication / authorisation of the source APIs.** Incorporator
  passes whatever credentials you give it. Bring your own rotation.
- **Sandboxing of user `outflow` modules.** `fjord()` and the CLI
  execute user-supplied Python from `outflow=…`. Treat that file as
  trusted code — don't load `outflow=` paths from untrusted sources.
- **Operating-system level isolation.** The supplied `Dockerfile` runs
  as a non-root user, but full container hardening (seccomp, AppArmor,
  read-only root FS) is the deployer's responsibility.

## Disclosure Credit

Reporters who follow this policy will be credited in the release notes
unless they request anonymity.
