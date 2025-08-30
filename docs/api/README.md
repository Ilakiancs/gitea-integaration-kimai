# API Documentation

This directory contains API documentation for the Gitea to Kimai integration.

## Overview

The integration provides several APIs for managing synchronization between Gitea and Kimai:

- **REST API** - HTTP endpoints for programmatic access
- **Webhook API** - Event-driven integration
- **CLI API** - Command-line interface

## REST API

### Authentication

The REST API uses JWT authentication. To obtain a token:

```bash
POST /api/v1/auth
Content-Type: application/json

{
  "username": "your_username",
  "password": "your_password"
}
```

### Endpoints

#### System Status
```
GET /api/v1/status
```

Returns system health and status information.

#### Synchronization
```
POST /api/v1/sync/trigger
```

Triggers a manual synchronization.

```
GET /api/v1/sync/status
```

Returns current synchronization status.

#### Activities
```
GET /api/v1/activities
```

Returns list of synchronized activities.

```
GET /api/v1/activities/{id}
```

Returns specific activity details.

#### Projects
```
GET /api/v1/projects
```

Returns list of synchronized projects.

```
GET /api/v1/projects/{id}
```

Returns specific project details.

#### Reports
```
GET /api/v1/reports/sync
```

Returns synchronization reports.

```
GET /api/v1/reports/export
```

Exports data in various formats.

## Webhook API

### Gitea Webhooks

The integration can receive webhooks from Gitea for real-time updates:

```
POST /webhooks/gitea
```

Supported events:
- `issues` - Issue created, updated, or closed
- `pull_request` - Pull request events
- `push` - Code push events

### Configuration

Configure webhooks in your Gitea repository settings:

1. Go to Repository Settings → Webhooks
2. Add new webhook
3. Set URL to: `https://your-domain.com/webhooks/gitea`
4. Select events to trigger webhook

## CLI API

### Commands

#### Sync
```bash
python main.py sync [options]
```

Options:
- `--dry-run` - Test run without making changes
- `--repos` - Specify repositories to sync
- `--include-prs` - Include pull requests

#### Diagnostics
```bash
python main.py diagnose [options]
```

Options:
- `--all` - Run all diagnostics
- `--network` - Check network connectivity
- `--api` - Check API connectivity
- `--database` - Check database

#### Reports
```bash
python main.py report [options]
```

Options:
- `--detailed` - Show detailed report
- `--export` - Export to file

#### Testing
```bash
python main.py test [options]
```

Options:
- `--gitea` - Test Gitea connection only
- `--kimai` - Test Kimai connection only

## Error Handling

All APIs return consistent error responses:

```json
{
  "error": "Error message",
  "code": "ERROR_CODE",
  "timestamp": "2023-01-01T00:00:00Z"
}
```

Common error codes:
- `AUTHENTICATION_FAILED` - Invalid credentials
- `PERMISSION_DENIED` - Insufficient permissions
- `VALIDATION_ERROR` - Invalid input data
- `SERVICE_UNAVAILABLE` - External service unavailable
- `INTERNAL_ERROR` - Internal server error

## Rate Limiting

API requests are rate-limited to prevent abuse:

- **Authenticated users**: 100 requests per minute
- **Unauthenticated users**: 10 requests per minute

Rate limit headers are included in responses:
- `X-RateLimit-Limit` - Request limit
- `X-RateLimit-Remaining` - Remaining requests
- `X-RateLimit-Reset` - Reset time

## Examples

### Python Client

```python
import requests

# Authenticate
response = requests.post('https://api.example.com/api/v1/auth', json={
    'username': 'user',
    'password': 'pass'
})
token = response.json()['token']

# Use API
headers = {'Authorization': f'Bearer {token}'}
response = requests.get('https://api.example.com/api/v1/activities', headers=headers)
activities = response.json()
```

### cURL Examples

```bash
# Get system status
curl -X GET https://api.example.com/api/v1/status

# Trigger sync
curl -X POST https://api.example.com/api/v1/sync/trigger \
  -H "Authorization: Bearer YOUR_TOKEN"

# Get activities
curl -X GET https://api.example.com/api/v1/activities \
  -H "Authorization: Bearer YOUR_TOKEN"
```

## Versioning

API versioning is handled through URL paths:

- Current version: `/api/v1/`
- Future versions: `/api/v2/`, `/api/v3/`, etc.

Breaking changes will only be introduced in new major versions.

## Support

For API support and questions:

1. Check the troubleshooting guide
2. Review error logs
3. Contact the development team
