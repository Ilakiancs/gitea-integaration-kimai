#!/usr/bin/env python3
"""
Plugin Manager for Gitea-Kimai Integration

This module provides an extensible plugin system that allows users to extend
the functionality of the sync system through custom plugins.
"""

import os
import sys
import json
import logging
import inspect
import importlib
import threading
from typing import Dict, List, Any, Optional, Callable, Type
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from enum import Enum

logger = logging.getLogger(__name__)

class PluginType(Enum):
    """Types of plugins supported."""
    TRANSFORMER = "transformer"
    VALIDATOR = "validator"
    FILTER = "filter"
    NOTIFIER = "notifier"
    AUTHENTICATOR = "authenticator"
    STORAGE = "storage"
    MIDDLEWARE = "middleware"

class PluginStatus(Enum):
    """Plugin status states."""
    LOADED = "loaded"
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"
    DISABLED = "disabled"

@dataclass
class PluginMetadata:
    """Plugin metadata structure."""
    name: str
    version: str
    author: str
    description: str
    plugin_type: PluginType
    dependencies: List[str] = field(default_factory=list)
    config_schema: Dict[str, Any] = field(default_factory=dict)
    entry_point: str = "main"
    enabled: bool = True
    priority: int = 50

@dataclass
class PluginInfo:
    """Complete plugin information."""
    metadata: PluginMetadata
    module: Any
    instance: Any
    status: PluginStatus
    error_message: Optional[str] = None
    load_time: Optional[float] = None

class PluginInterface(ABC):
    """Base interface for all plugins."""

    @abstractmethod
    def initialize(self, config: Dict[str, Any]) -> bool:
        """Initialize the plugin with configuration."""
        pass

    @abstractmethod
    def shutdown(self) -> bool:
        """Shutdown the plugin gracefully."""
        pass

    def get_metadata(self) -> PluginMetadata:
        """Get plugin metadata."""
        return PluginMetadata(
            name="BasePlugin",
            version="1.0.0",
            author="Unknown",
            description="Base plugin interface",
            plugin_type=PluginType.MIDDLEWARE
        )

class TransformerPlugin(PluginInterface):
    """Interface for data transformation plugins."""

    @abstractmethod
    def transform(self, data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Transform data from one format to another."""
        pass

class ValidatorPlugin(PluginInterface):
    """Interface for data validation plugins."""

    @abstractmethod
    def validate(self, data: Dict[str, Any], context: Dict[str, Any]) -> bool:
        """Validate data and return True if valid."""
        pass

    @abstractmethod
    def get_validation_errors(self) -> List[str]:
        """Get list of validation errors from last validation."""
        pass

class FilterPlugin(PluginInterface):
    """Interface for data filtering plugins."""

    @abstractmethod
    def filter(self, data: List[Dict[str, Any]], context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Filter data based on plugin criteria."""
        pass

class NotifierPlugin(PluginInterface):
    """Interface for notification plugins."""

    @abstractmethod
    def notify(self, message: str, level: str, context: Dict[str, Any]) -> bool:
        """Send notification message."""
        pass

class PluginManager:
    """Manages plugin lifecycle and execution."""

    def __init__(self, plugin_dir: str = "plugins", config_file: str = "plugin_config.json"):
        self.plugin_dir = Path(plugin_dir)
        self.config_file = config_file
        self.plugins: Dict[str, PluginInfo] = {}
        self.hooks: Dict[str, List[Callable]] = {}
        self.config: Dict[str, Any] = {}
        self.lock = threading.Lock()

        # Create plugin directory if it doesn't exist
        self.plugin_dir.mkdir(exist_ok=True)

        # Load configuration
        self._load_config()

        # Add plugin directory to Python path
        if str(self.plugin_dir) not in sys.path:
            sys.path.insert(0, str(self.plugin_dir))

    def _load_config(self):
        """Load plugin configuration."""
        config_path = Path(self.config_file)
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    self.config = json.load(f)
            except Exception as e:
                logger.error(f"Failed to load plugin config: {e}")
                self.config = {}
        else:
            self.config = {}

    def _save_config(self):
        """Save plugin configuration."""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save plugin config: {e}")

    def discover_plugins(self) -> List[str]:
        """Discover available plugins in the plugin directory."""
        plugins = []

        for item in self.plugin_dir.iterdir():
            if item.is_dir() and not item.name.startswith('_'):
                # Check for plugin.json metadata file
                metadata_file = item / "plugin.json"
                if metadata_file.exists():
                    plugins.append(item.name)
                else:
                    # Check for Python module with plugin interface
                    init_file = item / "__init__.py"
                    main_file = item / "main.py"
                    if init_file.exists() or main_file.exists():
                        plugins.append(item.name)

        return plugins

    def load_plugin_metadata(self, plugin_name: str) -> Optional[PluginMetadata]:
        """Load plugin metadata from plugin.json file."""
        metadata_file = self.plugin_dir / plugin_name / "plugin.json"

        if not metadata_file.exists():
            return None

        try:
            with open(metadata_file, 'r') as f:
                data = json.load(f)

            return PluginMetadata(
                name=data.get("name", plugin_name),
                version=data.get("version", "1.0.0"),
                author=data.get("author", "Unknown"),
                description=data.get("description", ""),
                plugin_type=PluginType(data.get("type", "middleware")),
                dependencies=data.get("dependencies", []),
                config_schema=data.get("config_schema", {}),
                entry_point=data.get("entry_point", "main"),
                enabled=data.get("enabled", True),
                priority=data.get("priority", 50)
            )

        except Exception as e:
            logger.error(f"Failed to load metadata for plugin {plugin_name}: {e}")
            return None

    def load_plugin(self, plugin_name: str) -> bool:
        """Load a plugin by name."""
        with self.lock:
            if plugin_name in self.plugins:
                logger.warning(f"Plugin {plugin_name} already loaded")
                return True

            try:
                import time
                start_time = time.time()

                # Load metadata
                metadata = self.load_plugin_metadata(plugin_name)
                if not metadata:
                    # Create default metadata
                    metadata = PluginMetadata(
                        name=plugin_name,
                        version="1.0.0",
                        author="Unknown",
                        description=f"Plugin {plugin_name}",
                        plugin_type=PluginType.MIDDLEWARE
                    )

                # Check if plugin is enabled
                if not metadata.enabled:
                    logger.info(f"Plugin {plugin_name} is disabled")
                    return False

                # Check dependencies
                if not self._check_dependencies(metadata.dependencies):
                    error_msg = f"Dependencies not met for plugin {plugin_name}"
                    logger.error(error_msg)
                    self.plugins[plugin_name] = PluginInfo(
                        metadata=metadata,
                        module=None,
                        instance=None,
                        status=PluginStatus.ERROR,
                        error_message=error_msg
                    )
                    return False

                # Import plugin module
                module_name = f"{plugin_name}.{metadata.entry_point}"
                try:
                    module = importlib.import_module(module_name)
                except ImportError:
                    # Try direct import
                    module = importlib.import_module(plugin_name)

                # Find plugin class
                plugin_class = self._find_plugin_class(module)
                if not plugin_class:
                    error_msg = f"No plugin class found in module {plugin_name}"
                    logger.error(error_msg)
                    self.plugins[plugin_name] = PluginInfo(
                        metadata=metadata,
                        module=module,
                        instance=None,
                        status=PluginStatus.ERROR,
                        error_message=error_msg
                    )
                    return False

                # Create plugin instance
                plugin_config = self.config.get(plugin_name, {})
                instance = plugin_class()

                # Initialize plugin
                if not instance.initialize(plugin_config):
                    error_msg = f"Failed to initialize plugin {plugin_name}"
                    logger.error(error_msg)
                    self.plugins[plugin_name] = PluginInfo(
                        metadata=metadata,
                        module=module,
                        instance=instance,
                        status=PluginStatus.ERROR,
                        error_message=error_msg
                    )
                    return False

                load_time = time.time() - start_time

                self.plugins[plugin_name] = PluginInfo(
                    metadata=metadata,
                    module=module,
                    instance=instance,
                    status=PluginStatus.LOADED,
                    load_time=load_time
                )

                logger.info(f"Successfully loaded plugin {plugin_name} in {load_time:.3f}s")
                return True

            except Exception as e:
                error_msg = f"Failed to load plugin {plugin_name}: {e}"
                logger.error(error_msg)
                self.plugins[plugin_name] = PluginInfo(
                    metadata=metadata if 'metadata' in locals() else None,
                    module=None,
                    instance=None,
                    status=PluginStatus.ERROR,
                    error_message=error_msg
                )
                return False

    def _find_plugin_class(self, module) -> Optional[Type]:
        """Find the plugin class in a module."""
        for name, obj in inspect.getmembers(module):
            if (inspect.isclass(obj) and
                issubclass(obj, PluginInterface) and
                obj != PluginInterface):
                return obj
        return None

    def _check_dependencies(self, dependencies: List[str]) -> bool:
        """Check if plugin dependencies are satisfied."""
        for dep in dependencies:
            try:
                importlib.import_module(dep)
            except ImportError:
                logger.error(f"Dependency {dep} not available")
                return False
        return True

    def unload_plugin(self, plugin_name: str) -> bool:
        """Unload a plugin."""
        with self.lock:
            if plugin_name not in self.plugins:
                logger.warning(f"Plugin {plugin_name} not loaded")
                return False

            plugin_info = self.plugins[plugin_name]

            try:
                # Shutdown plugin
                if plugin_info.instance:
                    plugin_info.instance.shutdown()

                # Remove from plugins
                del self.plugins[plugin_name]

                logger.info(f"Successfully unloaded plugin {plugin_name}")
                return True

            except Exception as e:
                logger.error(f"Failed to unload plugin {plugin_name}: {e}")
                return False

    def activate_plugin(self, plugin_name: str) -> bool:
        """Activate a loaded plugin."""
        with self.lock:
            if plugin_name not in self.plugins:
                logger.error(f"Plugin {plugin_name} not loaded")
                return False

            plugin_info = self.plugins[plugin_name]

            if plugin_info.status == PluginStatus.ACTIVE:
                logger.warning(f"Plugin {plugin_name} already active")
                return True

            if plugin_info.status != PluginStatus.LOADED:
                logger.error(f"Cannot activate plugin {plugin_name} with status {plugin_info.status}")
                return False

            plugin_info.status = PluginStatus.ACTIVE
            logger.info(f"Activated plugin {plugin_name}")
            return True

    def deactivate_plugin(self, plugin_name: str) -> bool:
        """Deactivate an active plugin."""
        with self.lock:
            if plugin_name not in self.plugins:
                logger.error(f"Plugin {plugin_name} not loaded")
                return False

            plugin_info = self.plugins[plugin_name]

            if plugin_info.status == PluginStatus.INACTIVE:
                logger.warning(f"Plugin {plugin_name} already inactive")
                return True

            plugin_info.status = PluginStatus.INACTIVE
            logger.info(f"Deactivated plugin {plugin_name}")
            return True

    def get_plugins_by_type(self, plugin_type: PluginType) -> List[str]:
        """Get list of plugin names by type."""
        return [
            name for name, info in self.plugins.items()
            if info.metadata and info.metadata.plugin_type == plugin_type
            and info.status == PluginStatus.ACTIVE
        ]

    def execute_transformers(self, data: Dict[str, Any], context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute all active transformer plugins."""
        context = context or {}
        result = data.copy()

        transformer_plugins = self.get_plugins_by_type(PluginType.TRANSFORMER)

        # Sort by priority
        transformer_plugins.sort(key=lambda name: self.plugins[name].metadata.priority)

        for plugin_name in transformer_plugins:
            try:
                plugin_instance = self.plugins[plugin_name].instance
                result = plugin_instance.transform(result, context)
            except Exception as e:
                logger.error(f"Error in transformer plugin {plugin_name}: {e}")

        return result

    def execute_validators(self, data: Dict[str, Any], context: Dict[str, Any] = None) -> bool:
        """Execute all active validator plugins."""
        context = context or {}

        validator_plugins = self.get_plugins_by_type(PluginType.VALIDATOR)

        for plugin_name in validator_plugins:
            try:
                plugin_instance = self.plugins[plugin_name].instance
                if not plugin_instance.validate(data, context):
                    logger.warning(f"Validation failed in plugin {plugin_name}")
                    errors = plugin_instance.get_validation_errors()
                    for error in errors:
                        logger.warning(f"Validation error: {error}")
                    return False
            except Exception as e:
                logger.error(f"Error in validator plugin {plugin_name}: {e}")
                return False

        return True

    def execute_filters(self, data: List[Dict[str, Any]], context: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Execute all active filter plugins."""
        context = context or {}
        result = data.copy()

        filter_plugins = self.get_plugins_by_type(PluginType.FILTER)

        # Sort by priority
        filter_plugins.sort(key=lambda name: self.plugins[name].metadata.priority)

        for plugin_name in filter_plugins:
            try:
                plugin_instance = self.plugins[plugin_name].instance
                result = plugin_instance.filter(result, context)
            except Exception as e:
                logger.error(f"Error in filter plugin {plugin_name}: {e}")

        return result

    def send_notification(self, message: str, level: str = "info", context: Dict[str, Any] = None):
        """Send notification through all active notifier plugins."""
        context = context or {}

        notifier_plugins = self.get_plugins_by_type(PluginType.NOTIFIER)

        for plugin_name in notifier_plugins:
            try:
                plugin_instance = self.plugins[plugin_name].instance
                plugin_instance.notify(message, level, context)
            except Exception as e:
                logger.error(f"Error in notifier plugin {plugin_name}: {e}")

    def register_hook(self, hook_name: str, callback: Callable):
        """Register a hook callback."""
        if hook_name not in self.hooks:
            self.hooks[hook_name] = []
        self.hooks[hook_name].append(callback)

    def execute_hook(self, hook_name: str, *args, **kwargs):
        """Execute all callbacks for a hook."""
        if hook_name in self.hooks:
            for callback in self.hooks[hook_name]:
                try:
                    callback(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Error in hook {hook_name}: {e}")

    def get_plugin_info(self, plugin_name: str) -> Optional[PluginInfo]:
        """Get information about a plugin."""
        return self.plugins.get(plugin_name)

    def list_plugins(self) -> Dict[str, Dict[str, Any]]:
        """List all plugins with their status."""
        result = {}

        for name, info in self.plugins.items():
            result[name] = {
                'status': info.status.value,
                'type': info.metadata.plugin_type.value if info.metadata else 'unknown',
                'version': info.metadata.version if info.metadata else 'unknown',
                'description': info.metadata.description if info.metadata else '',
                'error': info.error_message,
                'load_time': info.load_time
            }

        return result

    def load_all_plugins(self):
        """Load all discovered plugins."""
        plugins = self.discover_plugins()

        for plugin_name in plugins:
            self.load_plugin(plugin_name)

    def shutdown_all_plugins(self):
        """Shutdown all loaded plugins."""
        plugin_names = list(self.plugins.keys())

        for plugin_name in plugin_names:
            self.unload_plugin(plugin_name)

# Global plugin manager instance
_global_plugin_manager = None

def get_plugin_manager() -> PluginManager:
    """Get global plugin manager instance."""
    global _global_plugin_manager

    if _global_plugin_manager is None:
        _global_plugin_manager = PluginManager()

    return _global_plugin_manager
