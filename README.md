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

<!-- Copy-paste in your Readme.md file -->

<a href="https://next.ossinsight.io/widgets/official/analyze-repo-loc-per-month?repo_id=41986369" target="_blank" style="display: block" align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://next.ossinsight.io/widgets/official/analyze-repo-loc-per-month/thumbnail.png?repo_id=41986369&image_size=auto&color_scheme=dark" width="721" height="auto">
    <img alt="Lines of Code Changes of pingcap/tidb" src="https://next.ossinsight.io/widgets/official/analyze-repo-loc-per-month/thumbnail.png?repo_id=41986369&image_size=auto&color_scheme=light" width="721" height="auto">
  </picture>
</a>

<!-- Made with [OSS Insight](https://ossinsight.io/) -->
