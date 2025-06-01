#!/usr/bin/env python3
"""
Configuration Management System

Handles configuration loading, validation, and management for different
environments and deployment scenarios.
"""

import os
import json
import yaml
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path
from dataclasses import dataclass, asdict
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

@dataclass
class GiteaConfig:
    """Gitea configuration settings."""
    url: str
    token: str
    organization: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    verify_ssl: bool = True
    timeout: int = 30
    max_retries: int = 3
    request_delay: float = 0.5

@dataclass
class KimaiConfig:
    """Kimai configuration settings."""
    url: str
    username: str
    password: str
    token: Optional[str] = None
    verify_ssl: bool = True
    timeout: int = 30

@dataclass
class SyncConfig:
    """Sync operation configuration."""
    repositories: List[str]
    sync_pull_requests: bool = False
    read_only_mode: bool = False
    max_items_per_sync: int = 1000
    sync_interval_minutes: int = 60
    retry_attempts: int = 3
    retry_delay_seconds: int = 5

@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    path: str = "sync.db"
    backup_enabled: bool = True
    backup_retention_days: int = 30
    vacuum_on_startup: bool = False

@dataclass
class LoggingConfig:
    """Logging configuration settings."""
    level: str = "INFO"
    file_path: Optional[str] = None
    max_file_size_mb: int = 10
    backup_count: int = 5
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

@dataclass
class NotificationConfig:
    """Notification configuration settings."""
    email_enabled: bool = False
    email_smtp_server: Optional[str] = None
    email_smtp_port: int = 587
    email_username: Optional[str] = None
    email_password: Optional[str] = None
    email_recipients: List[str] = None
    slack_enabled: bool = False
    slack_webhook_url: Optional[str] = None
    discord_enabled: bool = False
    discord_webhook_url: Optional[str] = None

@dataclass
class RateLimitConfig:
    """Rate limiting configuration."""
    enabled: bool = True
    requests_per_minute: int = 60
    burst_limit: int = 10
    retry_after_seconds: int = 60

class ConfigurationManager:
    """Manages configuration loading and validation."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or "config"
        self.config_cache = {}
        self._load_environment()
    
    def _load_environment(self):
        """Load environment variables."""
        # Load .env file if it exists
        env_file = Path(".env")
        if env_file.exists():
            load_dotenv(env_file)
        
        # Load environment-specific .env file
        env = os.getenv("ENVIRONMENT", "development")
        env_file = Path(f".env.{env}")
        if env_file.exists():
            load_dotenv(env_file)
    
    def load_config(self, environment: str = None) -> Dict[str, Any]:
        """Load configuration for specified environment."""
        env = environment or os.getenv("ENVIRONMENT", "development")
        
        if env in self.config_cache:
            return self.config_cache[env]
        
        config = self._load_config_files(env)
        config = self._merge_environment_overrides(config, env)
        config = self._validate_config(config)
        
        self.config_cache[env] = config
        return config
    
    def _load_config_files(self, environment: str) -> Dict[str, Any]:
        """Load configuration from files."""
        config = {}
        
        # Load base config
        base_config_path = Path(self.config_path) / "config.yaml"
        if base_config_path.exists():
            with open(base_config_path, 'r') as f:
                config.update(yaml.safe_load(f) or {})
        
        # Load environment-specific config
        env_config_path = Path(self.config_path) / f"config.{environment}.yaml"
        if env_config_path.exists():
            with open(env_config_path, 'r') as f:
                env_config = yaml.safe_load(f) or {}
                config = self._deep_merge(config, env_config)
        
        # Load JSON config if YAML doesn't exist
        if not config:
            base_config_path = Path(self.config_path) / "config.json"
            if base_config_path.exists():
                with open(base_config_path, 'r') as f:
                    config.update(json.load(f) or {})
            
            env_config_path = Path(self.config_path) / f"config.{environment}.json"
            if env_config_path.exists():
                with open(env_config_path, 'r') as f:
                    env_config = json.load(f) or {}
                    config = self._deep_merge(config, env_config)
        
        return config
    
    def _merge_environment_overrides(self, config: Dict[str, Any], environment: str) -> Dict[str, Any]:
        """Merge environment variable overrides."""
        overrides = {}
        
        # Gitea overrides
        if os.getenv("GITEA_URL"):
            overrides.setdefault("gitea", {})["url"] = os.getenv("GITEA_URL")
        if os.getenv("GITEA_TOKEN"):
            overrides.setdefault("gitea", {})["token"] = os.getenv("GITEA_TOKEN")
        if os.getenv("GITEA_ORGANIZATION"):
            overrides.setdefault("gitea", {})["organization"] = os.getenv("GITEA_ORGANIZATION")
        
        # Kimai overrides
        if os.getenv("KIMAI_URL"):
            overrides.setdefault("kimai", {})["url"] = os.getenv("KIMAI_URL")
        if os.getenv("KIMAI_USERNAME"):
            overrides.setdefault("kimai", {})["username"] = os.getenv("KIMAI_USERNAME")
        if os.getenv("KIMAI_PASSWORD"):
            overrides.setdefault("kimai", {})["password"] = os.getenv("KIMAI_PASSWORD")
        if os.getenv("KIMAI_TOKEN"):
            overrides.setdefault("kimai", {})["token"] = os.getenv("KIMAI_TOKEN")
        
        # Sync overrides
        if os.getenv("REPOS_TO_SYNC"):
            overrides.setdefault("sync", {})["repositories"] = os.getenv("REPOS_TO_SYNC").split(",")
        if os.getenv("SYNC_PULL_REQUESTS"):
            overrides.setdefault("sync", {})["sync_pull_requests"] = os.getenv("SYNC_PULL_REQUESTS").lower() == "true"
        if os.getenv("READ_ONLY_MODE"):
            overrides.setdefault("sync", {})["read_only_mode"] = os.getenv("READ_ONLY_MODE").lower() == "true"
        
        # Database overrides
        if os.getenv("DATABASE_PATH"):
            overrides.setdefault("database", {})["path"] = os.getenv("DATABASE_PATH")
        
        # Logging overrides
        if os.getenv("LOG_LEVEL"):
            overrides.setdefault("logging", {})["level"] = os.getenv("LOG_LEVEL")
        
        return self._deep_merge(config, overrides)
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries."""
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def _validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate configuration and set defaults."""
        validated = {}
        
        # Validate Gitea config
        gitea_config = config.get("gitea", {})
        if not gitea_config.get("url"):
            raise ValueError("Gitea URL is required")
        if not gitea_config.get("token") and not (gitea_config.get("username") and gitea_config.get("password")):
            raise ValueError("Gitea authentication (token or username/password) is required")
        
        validated["gitea"] = GiteaConfig(**gitea_config)
        
        # Validate Kimai config
        kimai_config = config.get("kimai", {})
        if not kimai_config.get("url"):
            raise ValueError("Kimai URL is required")
        if not kimai_config.get("username") or not kimai_config.get("password"):
            raise ValueError("Kimai username and password are required")
        
        validated["kimai"] = KimaiConfig(**kimai_config)
        
        # Validate Sync config
        sync_config = config.get("sync", {})
        if not sync_config.get("repositories"):
            raise ValueError("At least one repository must be specified for sync")
        
        validated["sync"] = SyncConfig(**sync_config)
        
        # Set defaults for optional configs
        validated["database"] = DatabaseConfig(**config.get("database", {}))
        validated["logging"] = LoggingConfig(**config.get("logging", {}))
        validated["notifications"] = NotificationConfig(**config.get("notifications", {}))
        validated["rate_limit"] = RateLimitConfig(**config.get("rate_limit", {}))
        
        return validated
    
    def save_config(self, config: Dict[str, Any], environment: str = None):
        """Save configuration to file."""
        env = environment or os.getenv("ENVIRONMENT", "development")
        config_path = Path(self.config_path) / f"config.{env}.yaml"
        
        config_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert dataclasses to dictionaries
        config_dict = {}
        for key, value in config.items():
            if hasattr(value, '__dataclass_fields__'):
                config_dict[key] = asdict(value)
            else:
                config_dict[key] = value
        
        with open(config_path, 'w') as f:
            yaml.dump(config_dict, f, default_flow_style=False, indent=2)
        
        logger.info(f"Configuration saved to {config_path}")
    
    def get_config_section(self, section: str, environment: str = None) -> Any:
        """Get a specific configuration section."""
        config = self.load_config(environment)
        return config.get(section)
    
    def update_config_section(self, section: str, updates: Dict[str, Any], environment: str = None):
        """Update a specific configuration section."""
        config = self.load_config(environment)
        
        if section not in config:
            raise ValueError(f"Configuration section '{section}' not found")
        
        current_section = config[section]
        if hasattr(current_section, '__dataclass_fields__'):
            # Update dataclass
            for key, value in updates.items():
                if hasattr(current_section, key):
                    setattr(current_section, key, value)
        else:
            # Update dictionary
            config[section].update(updates)
        
        self.save_config(config, environment)
        self.config_cache.clear()  # Clear cache to reload
    
    def list_environments(self) -> List[str]:
        """List available configuration environments."""
        config_dir = Path(self.config_path)
        if not config_dir.exists():
            return ["development"]
        
        environments = set()
        for file_path in config_dir.glob("config.*.yaml"):
            env = file_path.stem.split(".", 1)[1]
            environments.add(env)
        
        for file_path in config_dir.glob("config.*.json"):
            env = file_path.stem.split(".", 1)[1]
            environments.add(env)
        
        return sorted(list(environments)) if environments else ["development"]
    
    def create_environment(self, environment: str, base_environment: str = "development"):
        """Create a new environment configuration."""
        base_config = self.load_config(base_environment)
        
        # Create environment-specific config
        env_config = {}
        for section, config_obj in base_config.items():
            if hasattr(config_obj, '__dataclass_fields__'):
                env_config[section] = asdict(config_obj)
            else:
                env_config[section] = config_obj
        
        self.save_config(env_config, environment)
        logger.info(f"Created new environment configuration: {environment}")
    
    def validate_connection_configs(self) -> Dict[str, bool]:
        """Validate connection configurations."""
        config = self.load_config()
        results = {}
        
        # Validate Gitea connection
        try:
            gitea_config = config["gitea"]
            # Basic URL validation
            if not gitea_config.url.startswith(('http://', 'https://')):
                results["gitea"] = False
            else:
                results["gitea"] = True
        except Exception as e:
            results["gitea"] = False
            logger.error(f"Gitea config validation failed: {e}")
        
        # Validate Kimai connection
        try:
            kimai_config = config["kimai"]
            if not kimai_config.url.startswith(('http://', 'https://')):
                results["kimai"] = False
            else:
                results["kimai"] = True
        except Exception as e:
            results["kimai"] = False
            logger.error(f"Kimai config validation failed: {e}")
        
        return results
