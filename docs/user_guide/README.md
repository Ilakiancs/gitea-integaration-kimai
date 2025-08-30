# User Guide

This guide provides step-by-step instructions for using the Gitea to Kimai integration.

## Getting Started

### Prerequisites

Before using the integration, ensure you have:

1. **Python 3.7+** installed
2. **Gitea instance** with API access
3. **Kimai instance** with API access
4. **Valid credentials** for both services

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-org/gitea-kimai-integration.git
   cd gitea-kimai-integration
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

### Configuration

Edit the `.env` file with your settings:

```env
# Gitea Configuration
GITEA_URL=https://your-gitea-instance.com
GITEA_TOKEN=your_gitea_api_token
GITEA_ORGANIZATION=your-organization

# Kimai Configuration
KIMAI_URL=https://your-kimai-instance.com
KIMAI_USERNAME=your_kimai_username
KIMAI_PASSWORD=your_kimai_password

# Repositories to sync
REPOS_TO_SYNC=repo1,repo2,repo3

# Database
DATABASE_PATH=sync.db

# Logging
LOG_LEVEL=INFO
```

## Basic Usage

### Running Synchronization

**Simple sync:**
```bash
python main.py sync
```

**Dry run (test without changes):**
```bash
python main.py sync --dry-run
```

**Sync specific repositories:**
```bash
python main.py sync --repos "repo1,repo2"
```

**Include pull requests:**
```bash
python main.py sync --include-prs
```

### Checking System Health

**Run diagnostics:**
```bash
python main.py diagnose
```

**Test connections:**
```bash
python main.py test
```

**Check system health:**
```bash
python main.py health
```

### Viewing Reports

**Show sync summary:**
```bash
python main.py report
```

**Detailed report:**
```bash
python main.py report --detailed
```

**Export report:**
```bash
python main.py report --export report.csv
```

## Advanced Usage

### Web Dashboard

Start the web dashboard for visual monitoring:

```bash
python main.py dashboard --port 8080
```

Access the dashboard at: `http://localhost:8080`

### API Server

Start the REST API server:

```bash
python main.py api --port 5000
```

API documentation available at: `http://localhost:5000/docs`

### Backup and Restore

**Create backup:**
```bash
python main.py backup create
```

**List backups:**
```bash
python main.py backup list
```

**Restore backup:**
```bash
python main.py backup restore --file backup.zip
```

## Automation

### Scheduled Sync

Set up automated synchronization using cron:

```bash
# Edit crontab
crontab -e

# Add hourly sync
0 * * * * cd /path/to/gitea-kimai-integration && python main.py sync

# Add daily backup
0 2 * * * cd /path/to/gitea-kimai-integration && python main.py backup create
```

### Systemd Service

Create a systemd service for continuous operation:

```ini
# /etc/systemd/system/gitea-kimai-sync.service
[Unit]
Description=Gitea to Kimai Sync Service
After=network.target

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/gitea-kimai-integration
ExecStart=/usr/bin/python3 main.py api --port 5000
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start the service:
```bash
sudo systemctl enable gitea-kimai-sync
sudo systemctl start gitea-kimai-sync
```

## Troubleshooting

### Common Issues

**Authentication Errors:**
- Verify API tokens are correct
- Check user permissions in both systems
- Ensure URLs are accessible

**Database Errors:**
- Check file permissions on sync.db
- Verify SQLite is installed
- Try recreating the database

**Network Issues:**
- Test connectivity to both services
- Check firewall settings
- Verify DNS resolution

### Debug Mode

Enable debug logging for detailed troubleshooting:

```bash
LOG_LEVEL=DEBUG python main.py sync
```

### Getting Help

1. **Check logs:** Review sync.log for error details
2. **Run diagnostics:** Use `python main.py diagnose --all`
3. **Test connections:** Use `python main.py test`
4. **Review documentation:** Check README.md and DIAGNOSTICS.md

## Best Practices

### Security

1. **Use environment variables** for sensitive data
2. **Rotate API tokens** regularly
3. **Limit API permissions** to minimum required
4. **Use HTTPS** for all connections
5. **Regular backups** of configuration and data

### Performance

1. **Schedule syncs** during off-peak hours
2. **Monitor resource usage** during sync
3. **Use rate limiting** to avoid API throttling
4. **Clean up old logs** regularly
5. **Optimize database** queries

### Monitoring

1. **Set up health checks** for automated monitoring
2. **Monitor sync success rates**
3. **Track API response times**
4. **Alert on failures**
5. **Regular maintenance** of the system

## Integration Examples

### CI/CD Integration

Add sync to your CI/CD pipeline:

```yaml
# .github/workflows/sync.yml
name: Sync to Kimai
on:
  issues:
    types: [opened, edited, closed]
  pull_request:
    types: [opened, edited, closed]

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Setup Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run sync
        run: python main.py sync
        env:
          GITEA_URL: ${{ secrets.GITEA_URL }}
          GITEA_TOKEN: ${{ secrets.GITEA_TOKEN }}
          KIMAI_URL: ${{ secrets.KIMAI_URL }}
          KIMAI_USERNAME: ${{ secrets.KIMAI_USERNAME }}
          KIMAI_PASSWORD: ${{ secrets.KIMAI_PASSWORD }}
```

### Docker Integration

Create a Dockerfile for containerized deployment:

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 5000

CMD ["python", "main.py", "api", "--host", "0.0.0.0", "--port", "5000"]
```

## Support

For additional support:

1. **Documentation:** Check README.md and other docs
2. **Issues:** Report bugs on GitHub
3. **Discussions:** Use GitHub Discussions for questions
4. **Email:** Contact the development team

## Contributing

We welcome contributions! Please see CONTRIBUTING.md for guidelines.
