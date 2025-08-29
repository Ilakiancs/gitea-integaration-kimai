# Development Notes
## Usage Examples

Basic sync:
```bash
python main.py sync
```
## Troubleshooting

### Common Issues

- Connection timeout: Check network settings
## Performance Tips

- Use async mode for better throughput
- Enable caching for frequently accessed data
## Configuration

### Environment Variables

- `DEBUG`: Enable debug mode
## Testing

### Run Tests

```bash
python -m pytest tests/
```
## Code Quality

### Linting

```bash
flake8 src/
mypy src/
```
## Deployment

### Requirements

- Python 3.8+
- Required packages from requirements.txt
## API Documentation

### Endpoints

- `GET /api/v1/status` - System status
- `POST /api/v1/sync` - Trigger sync
