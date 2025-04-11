# Gitea-Kimai Integration

A comprehensive enterprise-grade integration platform that bridges the gap between your Git workflow and time tracking by automatically syncing issues, commits, and project data between Gitea and Kimai. Built for teams that need their development time to reflect in their project management without manual overhead.

## Features

### Core Synchronization
- **Bidirectional Sync**: Real-time synchronization between Gitea issues and Kimai timesheet entries
- **Smart Conflict Resolution**: Automatic handling of data conflicts with configurable resolution strategies  
- **Incremental Updates**: Efficient syncing of only changed data to minimize resource usage
- **Batch Processing**: High-performance batch operations for large datasets
- **Data Validation**: Comprehensive validation with configurable rules and transformations

### Enterprise Features
- **Multi-Tenancy**: Full isolation and resource management for multiple organizations
- **Advanced Security**: Comprehensive audit logging, authentication, and authorization
- **Plugin System**: Extensible architecture with custom plugins for transformations and validations
- **Real-time Updates**: WebSocket support for instant notifications and live dashboard updates
- **GraphQL API**: Flexible query interface for advanced data access patterns

### Monitoring & Operations
- **Health Monitoring**: Comprehensive system health checks and alerting
- **Performance Metrics**: Detailed analytics and performance tracking
- **Backup & Recovery**: Automated backups with incremental and full backup strategies
- **Rate Limiting**: Adaptive rate limiting to protect external APIs
- **Export Utilities**: Multi-format data export (CSV, JSON, Excel, PDF, HTML)

### Developer Tools
- **CLI Interface**: Rich command-line tools for administration and troubleshooting
- **Test Framework**: Comprehensive test suite with unit, integration, and end-to-end tests
- **Configuration Management**: Environment-aware configuration with validation
- **Advanced Search**: Full-text search with filtering, faceting, and fuzzy matching

## Quick Start

### Prerequisites
- Python 3.8+
- SQLite 3.x
- Access to Gitea and Kimai instances

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/gitea-integaration-kimai.git
cd gitea-integaration-kimai

# Install dependencies
pip install -r requirements.txt

# Copy and configure settings
cp examples/config.example.json config.json
# Edit config.json with your endpoints and credentials

# Initialize the system
python main.py init

# Run your first sync
python main.py sync --verbose
```

### Environment Variables

Set these environment variables or configure them in your config file:

```bash
export GITEA_URL="https://your-gitea-instance.com"
export GITEA_TOKEN="your-gitea-access-token"
export KIMAI_URL="https://your-kimai-instance.com"
export KIMAI_USERNAME="your-username"
export KIMAI_PASSWORD="your-password"
export ENVIRONMENT="production"
```

## Configuration

### Basic Configuration

```json
{
  "gitea": {
    "url": "https://your-gitea-instance.com",
    "token": "your-gitea-access-token",
    "organization": "your-org",
    "repositories": ["repo1", "repo2"]
  },
  "kimai": {
    "url": "https://your-kimai-instance.com", 
    "username": "your-username",
    "password": "your-password",
    "default_project_id": 1
  },
  "sync": {
    "interval": 300,
    "batch_size": 50,
    "retry_attempts": 3,
    "enable_incremental": true,
    "sync_pull_requests": true
  },
  "database": {
    "path": "sync.db",
    "backup_interval": 86400
  },
  "logging": {
    "level": "INFO",
    "file": "sync.log",
    "max_size": "10MB",
    "backup_count": 5
  }
}
```

### Advanced Configuration

```yaml
# config/config.production.yaml
gitea:
  url: ${GITEA_URL}
  token: ${GITEA_TOKEN}
  rate_limit:
    requests_per_minute: 60
    burst_size: 10

kimai:
  url: ${KIMAI_URL}
  username: ${KIMAI_USERNAME}
  password: ${KIMAI_PASSWORD}
  connection_pool:
    max_connections: 10
    timeout: 30

sync:
  repositories:
    - name: "frontend"
      project_id: 1
      activities: ["Development", "Bug Fixing"]
    - name: "backend" 
      project_id: 2
      activities: ["API Development", "Database"]
  
  transformations:
    issue_to_timesheet:
      duration_calculation: "estimated"
      default_activity: "Development"
      
notifications:
  email:
    enabled: true
    smtp_server: "smtp.company.com"
    from_email: "sync@company.com"
  slack:
    enabled: true
    webhook_url: "${SLACK_WEBHOOK_URL}"

security:
  audit_logging: true
  webhook_validation: true
  rate_limiting: true
```

## Usage

### Command Line Interface

```bash
# Basic synchronization
python main.py sync

# Sync specific repositories
python main.py sync --repos "repo1,repo2"

# Dry run (no changes made)
python main.py sync --dry-run

# Sync with real-time monitoring
python main.py sync --monitor

# Manual sync operation
python main.py sync --type manual --force

# View sync status and history
python main.py status
python main.py history --days 7

# Run diagnostics
python main.py diagnose --all
python main.py diagnose --network --api

# Generate reports
python main.py report --format json --export report.json
python main.py report --detailed --days 30

# Backup operations
python main.py backup create
python main.py backup list
python main.py backup restore --file backup_20250411.zip

# Health monitoring
python main.py health --monitor
python main.py health --export health_report.json

# Start services
python main.py dashboard --port 8080
python main.py api --host 0.0.0.0 --port 8000
python main.py realtime --port 8765

# Plugin management
python main.py plugin list
python main.py plugin install custom_transformer
python main.py plugin enable notification_slack

# Multi-tenant operations
python main.py tenant create --name "acme-corp" --plan enterprise
python main.py tenant list --status active
python main.py tenant usage --tenant acme-corp

# Advanced features
python main.py graphql --query "{ repositories { name issuesCount } }"
python main.py export --format excel --data sync --filters '{"repository": "main"}'
python main.py search --text "bug fix" --repository "frontend"
```

### Python API

```python
from src.api.api_client import GiteaKimaiClient
from src.core.sync_engine import SyncEngine, SyncConfig
from src.monitoring.metrics import MetricsCollector
from src.utils.search_engine import AdvancedSearchEngine

# Initialize client
client = GiteaKimaiClient(
    base_url="http://localhost:8080",
    username="admin",
    password="secret"
)

# Authenticate and get status
client.authenticate()
status = client.get_sync_status()

# Trigger synchronization
sync_result = client.trigger_sync(
    repository="my-repo",
    dry_run=False
)

# Advanced sync configuration
config = SyncConfig(
    sync_interval=300,
    batch_size=100,
    enable_incremental=True,
    data_validation=True
)

# Create sync engine with custom clients
sync_engine = SyncEngine(config, gitea_client, kimai_client)
sync_engine.start()

# Collect and analyze metrics
metrics = MetricsCollector()
stats = metrics.get_sync_statistics(days=7)
print(f"Success rate: {stats['overall']['success_rate']:.1f}%")

# Advanced search capabilities
search_engine = AdvancedSearchEngine()
results = search_engine.search("sync_items", SearchQuery(
    search_text="bug fix",
    filters=[
        SearchFilter("repository", SearchOperator.EQUALS, "frontend"),
        SearchFilter("status", SearchOperator.IN, ["completed", "pending"])
    ],
    sort_criteria=[
        SortCriteria("updated", SortOrder.DESC)
    ],
    limit=50
))
```

### GraphQL API

```graphql
# Get repositories with issue counts
query GetRepositories {
  repositories(limit: 10) {
    id
    name
    description
    issuesCount: issues {
      totalCount
    }
  }
}

# Search issues with filters
query SearchIssues($filters: IssueFilterInput) {
  issues(filters: $filters, limit: 20) {
    id
    number
    title
    state
    repository {
      name
    }
    user {
      username
    }
  }
}

# Trigger sync operation
mutation TriggerSync($input: SyncOperationInput!) {
  triggerSync(input: $input) {
    success
    message
    operation {
      id
      status
      startedAt
    }
  }
}

# Subscribe to sync updates
subscription SyncUpdates($operationId: String) {
  syncStatusUpdate(operationId: $operationId) {
    id
    status
    itemsProcessed
    itemsSynced
  }
}

# Get system metrics
query GetMetrics($periodHours: Int) {
  metrics(periodHours: $periodHours) {
    totalSyncs
    successfulSyncs
    successRate
    averageDuration
    lastSyncTime
  }
}
```

### REST API

```bash
# Authentication
curl -X POST http://localhost:8080/api/v1/auth \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "secret"}'

# Get sync status
curl -H "Authorization: Bearer TOKEN" \
  http://localhost:8080/api/v1/status

# Trigger sync
curl -X POST http://localhost:8080/api/v1/sync/trigger \
  -H "Authorization: Bearer TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"repository": "my-repo", "dry_run": false}'

# Get activities
curl -H "Authorization: Bearer TOKEN" \
  "http://localhost:8080/api/v1/activities?repository=frontend&limit=50"

# Get sync history
curl -H "Authorization: Bearer TOKEN" \
  "http://localhost:8080/api/v1/sync/history?limit=100"

# Export data
curl -H "Authorization: Bearer TOKEN" \
  "http://localhost:8080/api/v1/export?format=csv&data=sync_items" \
  -o sync_data.csv
```

## Architecture

### System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     Gitea-Kimai Integration                     │
├─────────────────────────────────────────────────────────────────┤
│  CLI Interface  │  Web Dashboard  │  REST API  │  GraphQL API   │
├─────────────────────────────────────────────────────────────────┤
│              Real-time WebSocket Layer                         │
├─────────────────────────────────────────────────────────────────┤
│  Plugin System  │  Search Engine  │  Export Manager           │
├─────────────────────────────────────────────────────────────────┤
│              Core Sync Engine & Scheduler                      │
├─────────────────────────────────────────────────────────────────┤
│ Data Transform │ Validation │ Conflict Resolution │ Rate Limit │
├─────────────────────────────────────────────────────────────────┤
│  Monitoring    │  Metrics   │  Audit Logging │  Health Check  │
├─────────────────────────────────────────────────────────────────┤
│  Connection Pool │ Backup Manager │ Multi-Tenant │ Security   │
├─────────────────────────────────────────────────────────────────┤
│              Storage Layer (SQLite + File System)              │
├─────────────────────────────────────────────────────────────────┤
│           External APIs (Gitea REST + Kimai REST)              │
└─────────────────────────────────────────────────────────────────┘
```

### Key Components

- **Sync Engine**: Core synchronization logic with conflict resolution
- **API Layer**: REST and GraphQL endpoints for external integration  
- **Plugin System**: Extensible architecture for custom functionality
- **Multi-Tenancy**: Complete tenant isolation and resource management
- **Real-time Layer**: WebSocket support for live updates
- **Security**: Audit logging, authentication, and authorization
- **Monitoring**: Health checks, metrics collection, and alerting

## Testing

### Running Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test categories
python tests/test_framework.py --class TestSyncEngine
python tests/test_framework.py --class TestAPIClient --method test_authentication

# Run tests with coverage
python tests/test_framework.py --coverage

# Integration tests
python -m pytest tests/integration/ -v

# Performance tests
python -m pytest tests/performance/ -v --benchmark-only
```

### Test Configuration

```python
# tests/conftest.py
import pytest
from src.core.sync_engine import SyncEngine
from tests.test_framework import MockGiteaClient, MockKimaiClient

@pytest.fixture
def sync_engine():
    config = SyncConfig(batch_size=10, max_retries=3)
    gitea_client = MockGiteaClient()
    kimai_client = MockKimaiClient()
    return SyncEngine(config, gitea_client, kimai_client)

@pytest.fixture  
def test_database():
    # Create temporary test database
    pass
```

## Deployment

### Docker Deployment

```dockerfile
# Dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8080 8000 8765

CMD ["python", "main.py", "api", "--host", "0.0.0.0"]
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  sync-api:
    build: .
    ports:
      - "8080:8080"
    environment:
      - ENVIRONMENT=production
      - GITEA_URL=${GITEA_URL}
      - KIMAI_URL=${KIMAI_URL}
    volumes:
      - ./data:/app/data
      - ./config:/app/config
    
  sync-dashboard:
    build: .
    command: python main.py dashboard --host 0.0.0.0 --port 8000
    ports:
      - "8000:8000"
    depends_on:
      - sync-api
      
  sync-realtime:
    build: .
    command: python main.py realtime --host 0.0.0.0 --port 8765
    ports:
      - "8765:8765"
    depends_on:
      - sync-api
```

### Kubernetes Deployment

```yaml
# k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gitea-kimai-sync
spec:
  replicas: 3
  selector:
    matchLabels:
      app: gitea-kimai-sync
  template:
    metadata:
      labels:
        app: gitea-kimai-sync
    spec:
      containers:
      - name: sync-api
        image: gitea-kimai-sync:latest
        ports:
        - containerPort: 8080
        env:
        - name: ENVIRONMENT
          value: "production"
        - name: GITEA_URL
          valueFrom:
            secretKeyRef:
              name: sync-secrets
              key: gitea-url
        volumeMounts:
        - name: config-volume
          mountPath: /app/config
        - name: data-volume
          mountPath: /app/data
      volumes:
      - name: config-volume
        configMap:
          name: sync-config
      - name: data-volume
        persistentVolumeClaim:
          claimName: sync-data-pvc
```

### Production Considerations

- **Security**: Use HTTPS, strong authentication, and audit logging
- **Monitoring**: Set up health checks, metrics collection, and alerting
- **Backup**: Configure automated backups with retention policies
- **Scaling**: Use load balancers and multiple instances for high availability
- **Database**: Consider PostgreSQL for larger deployments
- **Caching**: Implement Redis for session management and caching

## Troubleshooting

### Common Issues

**Connection Issues**
```bash
# Test connectivity
python main.py diagnose --network
python main.py test --gitea --kimai

# Check logs
tail -f sync.log
grep ERROR sync.log
```

**Sync Failures**
```bash
# Check sync status
python main.py status

# Run diagnostics
python main.py diagnose --all

# View error details
python main.py history --errors-only
```

**Performance Issues**
```bash
# Check metrics
python main.py report --performance

# Monitor resource usage
python main.py health --monitor

# Analyze slow queries
python main.py diagnose --database
```

### Debug Mode

```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
python main.py sync --verbose

# Run in development mode
python main.py --dev sync

# Enable SQL query logging
export DEBUG_SQL=true
python main.py sync
```

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Setup

```bash
# Clone and setup
git clone https://github.com/your-org/gitea-integaration-kimai.git
cd gitea-integaration-kimai

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Setup pre-commit hooks
pre-commit install

# Run tests
python -m pytest tests/
```

### Code Style

- Follow PEP 8 guidelines
- Use type hints
- Write comprehensive docstrings
- Add unit tests for new features
- Update documentation

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- **Documentation**: [https://docs.gitea-kimai-sync.com](https://docs.gitea-kimai-sync.com)
- **Issues**: [GitHub Issues](https://github.com/your-org/gitea-integaration-kimai/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-org/gitea-integaration-kimai/discussions)
- **Email**: support@gitea-kimai-sync.com

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history and release notes.