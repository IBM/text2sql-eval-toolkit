# Security Policy

## Supported Versions

The following versions of the Text2SQL Evaluation Toolkit are currently supported with security updates:

| Version | Supported          |
| ------- | ------------------ |
| 1.x.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

To report a security issue, please email the maintainers with a description of the issue, the steps you took to create the issue, affected versions, and if known, mitigations for the issue.

Our vulnerability management team will acknowledge receiving your email within 3 working days. This project follows a 90 day disclosure timeline.

### What to Include in Your Report

- Type of issue (e.g., buffer overflow, SQL injection, cross-site scripting, etc.)
- Full paths of source file(s) related to the manifestation of the issue
- The location of the affected source code (tag/branch/commit or direct URL)
- Any special configuration required to reproduce the issue
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the issue, including how an attacker might exploit it

### What to Expect

- A confirmation of receipt of your vulnerability report
- An assessment of the vulnerability and its impact
- An estimated timeline for a fix
- Notification when the vulnerability is fixed
- Public acknowledgment of your responsible disclosure (if desired)

## Security Best Practices

When using the Text2SQL Evaluation Toolkit:

1. **Environment Variables**: Store sensitive credentials (API keys, database passwords) in environment variables, never in code
2. **Database Connections**: Use secure connection strings and limit database user permissions
3. **Input Validation**: Validate and sanitize all user inputs before processing
4. **Dependencies**: Keep dependencies up to date and monitor for security advisories
5. **Access Control**: Implement appropriate access controls for production deployments

## Disclosure Policy

When we receive a security bug report, we will:

1. Confirm the problem and determine affected versions
2. Audit code to find any similar problems
3. Prepare fixes for all supported versions
4. Release new versions as soon as possible

Thank you for helping keep the Text2SQL Evaluation Toolkit and its users safe!