# Security Best Practices

## ⚠️ CRITICAL: Exposed Credentials

**This repository previously contained hardcoded API keys and service tokens in the git history.**

If you are the repository owner, you must **immediately rotate all exposed credentials**:

1. **Anthropic API Key** (`ANTHROPIC_API_KEY`)
   - Revoke the exposed key (check git history for the compromised value)
   - Generate a new key from your Anthropic console
   - Update environment variables with the new key

2. **Databricks Token** (`DATABRICKS_TOKEN`)
   - Revoke the exposed token (check git history for the compromised value)
   - Generate a new token from your Databricks workspace
   - Update environment variables with the new token

**Why rotation is necessary:** Even though these secrets have been removed from the code, they still exist in the git commit history and may have been exposed. Anyone with access to the repository history can potentially retrieve and misuse these credentials.

## Secret Management Guidelines

### ❌ NEVER Store Secrets in Code

**DO NOT** hardcode any of the following in your source code:
- API keys
- Service tokens
- Passwords
- Private keys
- OAuth secrets
- Database connection strings with credentials

### ✅ Use Environment Variables

All secrets should be provided via environment variables:

```python
# ✅ CORRECT: Use environment variables with no fallback
api_key = os.environ["API_KEY"]

# ❌ WRONG: Hardcoded fallback exposes secrets
api_key = os.environ.get("API_KEY", "hardcoded-secret-key")
```

Using `os.environ["KEY"]` without a fallback will raise a `KeyError` if the environment variable is not set, which is the desired behavior. This enforces proper configuration and prevents accidental exposure.

### Required Environment Variables

The following environment variables must be set before running the code generation scripts:

- `ANTHROPIC_API_KEY` - Your Anthropic API key for Claude
- `DATABRICKS_TOKEN` - Your Databricks personal access token
- `DATABRICKS_HOST` - Your Databricks workspace URL (has a fallback in code for convenience)

### Setting Environment Variables

**For local development:**
```bash
export ANTHROPIC_API_KEY="your-key-here"
export DATABRICKS_TOKEN="your-token-here"
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
```

**For production/CI environments:**
- Use secure secret management systems (AWS Secrets Manager, Azure Key Vault, HashiCorp Vault, etc.)
- Use your CI/CD platform's secret management (GitHub Secrets, GitLab CI/CD Variables, etc.)
- Never log or print environment variables containing secrets

## Additional Security Recommendations

1. **Use `.gitignore`**: Ensure sensitive files are excluded from version control
2. **Enable Secret Scanning**: Use GitHub's secret scanning feature or similar tools
3. **Regular Audits**: Periodically review your code for accidentally committed secrets
4. **Least Privilege**: Only grant the minimum necessary permissions to API keys and tokens
5. **Monitor Usage**: Regularly review API usage logs for suspicious activity
6. **Rotate Regularly**: Establish a schedule for rotating credentials even if not compromised

## Reporting Security Issues

If you discover a security vulnerability in this repository, please report it responsibly by contacting the repository maintainer directly rather than opening a public issue.
