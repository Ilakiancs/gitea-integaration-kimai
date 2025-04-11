# Gitea-Kimai Integration

bridges the gap between your git workflow and time tracking by automatically syncing issues, commits, and project data between Gitea and Kimai. built for teams that need their development time to reflect in their project management without manual overhead.

## what it does

syncs issues from Gitea to Kimai activities, tracks time against the right projects, and keeps everything in sync bidirectionally. handles data transformation, validation, and conflict resolution automatically. supports both manual and scheduled synchronization.

## getting started

```bash
pip install -r requirements.txt
cp examples/config.example.json config.json
# edit config.json with your endpoints
python main.py sync --verbose
```

## configuration

```json
{
  "gitea": {
    "url": "https://your-gitea-instance.com",
    "token": "your-gitea-access-token"
  },
  "kimai": {
    "url": "https://your-kimai-instance.com", 
    "username": "your-username",
    "password": "your-password"
  },
  "sync": {
    "interval": 300,
    "batch_size": 50,
    "retry_attempts": 3
  }
}
```

## using the api

```python
from src.api.api_client import GiteaKimaiClient

client = GiteaKimaiClient(config)
status = client.get_sync_status()
client.trigger_sync()
```

## architecture

core sync engine handles the heavy lifting, data pipeline transforms between systems, storage layer manages caching and persistence, api layer provides rest endpoints and webhooks, monitoring tracks performance and health, error handling ensures robust operation.

## development

```bash
python -m pytest tests/
python src/main.py --dev
python -m src.diagnostics.system_check
python main.py test
```

## license

MIT License - see LICENSE file for details.