#!/usr/bin/env python3
"""
Plugin System for Gitea-Kimai Integration

Provides a flexible plugin architecture for extending the integration
with custom data processors, transformers, and external service connectors.
"""

import os
import sys
import json
import logging
import importlib
import inspect
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Type, Callable
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

@dataclass
class PluginMetadata:
    """Plugin metadata information."""
    name: str
    version: str
    description: str
    author: str
    dependencies: List[str] = None
    config_schema: Dict[str, Any] = None
    enabled: bool = True

class PluginBase(ABC):
    """Base class for all plugins."""

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.logger = logging.getLogger(f"plugin.{self.__class__.__name__}")

    @abstractmethod
    def get_metadata(self) -> PluginMetadata:
        """Return plugin metadata."""
        pass

    @abstractmethod
    async def initialize(self) -> bool:
        """Initialize the plugin. Return True if successful."""
        pass

    @abstractmethod
    async def cleanup(self) -> None:
        """Cleanup plugin resources."""
        pass

    def validate_config(self) -> bool:
        """Validate plugin configuration."""
        return True

class DataProcessorPlugin(PluginBase):
    """Base class for data processing plugins."""

    @abstractmethod
    async def process_issue(self, repository: str, issue: Dict[str, Any]) -> Dict[str, Any]:
        """Process issue data before sync."""
        pass

    @abstractmethod
    async def process_pull_request(self, repository: str, pr: Dict[str, Any]) -> Dict[str, Any]:
        """Process pull request data before sync."""
        pass

    @abstractmethod
    async def process_commit(self, repository: str, commit: Dict[str, Any]) -> Dict[str, Any]:
        """Process commit data before sync."""
        pass

class TransformerPlugin(PluginBase):
    """Base class for data transformation plugins."""

    @abstractmethod
    async def transform_to_kimai(self, data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """Transform data for Kimai API."""
        pass

    @abstractmethod
    async def transform_from_kimai(self, data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """Transform data from Kimai API."""
        pass

class NotificationPlugin(PluginBase):
    """Base class for notification plugins."""

    @abstractmethod
    async def send_notification(self, message: str, level: str = "info", metadata: Dict[str, Any] = None) -> bool:
        """Send notification."""
        pass

    @abstractmethod
    async def send_sync_report(self, report: Dict[str, Any]) -> bool:
        """Send sync completion report."""
        pass

class ExternalServicePlugin(PluginBase):
    """Base class for external service integration plugins."""

    @abstractmethod
    async def sync_to_service(self, data: Dict[str, Any]) -> bool:
        """Sync data to external service."""
        pass

    @abstractmethod
    async def sync_from_service(self) -> List[Dict[str, Any]]:
        """Sync data from external service."""
        pass

class ValidationPlugin(PluginBase):
    """Base class for validation plugins."""

    @abstractmethod
    async def validate_data(self, data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """Validate data and return validation result."""
        pass

class PluginHook:
    """Represents a plugin hook point."""

    def __init__(self, name: str, plugin_type: Type[PluginBase]):
        self.name = name
        self.plugin_type = plugin_type
        self.plugins: List[PluginBase] = []

    def register_plugin(self, plugin: PluginBase) -> bool:
        """Register a plugin for this hook."""
        if not isinstance(plugin, self.plugin_type):
            logger.error(f"Plugin {plugin.__class__.__name__} is not compatible with hook {self.name}")
            return False

        self.plugins.append(plugin)
        logger.info(f"Registered plugin {plugin.__class__.__name__} for hook {self.name}")
        return True

    def unregister_plugin(self, plugin: PluginBase) -> bool:
        """Unregister a plugin from this hook."""
        if plugin in self.plugins:
            self.plugins.remove(plugin)
            logger.info(f"Unregistered plugin {plugin.__class__.__name__} from hook {self.name}")
            return True
        return False

    async def execute_plugins(self, method_name: str, *args, **kwargs) -> List[Any]:
        """Execute a method on all registered plugins."""
        results = []
        for plugin in self.plugins:
            try:
                method = getattr(plugin, method_name, None)
                if method and callable(method):
                    if inspect.iscoroutinefunction(method):
                        result = await method(*args, **kwargs)
                    else:
                        result = method(*args, **kwargs)
                    results.append(result)
            except Exception as e:
                logger.error(f"Error executing {method_name} on plugin {plugin.__class__.__name__}: {e}")

        return results

class PluginManager:
    """Manages plugin lifecycle and execution."""

    def __init__(self, plugin_directories: List[str] = None):
        self.plugin_directories = plugin_directories or ["plugins"]
        self.loaded_plugins: Dict[str, PluginBase] = {}
        self.plugin_classes: Dict[str, Type[PluginBase]] = {}
        self.hooks: Dict[str, PluginHook] = {}
        self.config: Dict[str, Any] = {}

        # Register default hooks
        self._register_default_hooks()

    def _register_default_hooks(self):
        """Register default plugin hooks."""
        self.hooks["data_processor"] = PluginHook("data_processor", DataProcessorPlugin)
        self.hooks["transformer"] = PluginHook("transformer", TransformerPlugin)
        self.hooks["notification"] = PluginHook("notification", NotificationPlugin)
        self.hooks["external_service"] = PluginHook("external_service", ExternalServicePlugin)
        self.hooks["validation"] = PluginHook("validation", ValidationPlugin)

    def register_hook(self, name: str, plugin_type: Type[PluginBase]) -> bool:
        """Register a new plugin hook."""
        if name in self.hooks:
            logger.warning(f"Hook {name} already exists")
            return False

        self.hooks[name] = PluginHook(name, plugin_type)
        logger.info(f"Registered new hook: {name}")
        return True

    def load_config(self, config_path: str = "plugins/config.json"):
        """Load plugin configuration."""
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    self.config = json.load(f)
                logger.info(f"Loaded plugin configuration from {config_path}")
            else:
                logger.info(f"No plugin configuration found at {config_path}")
        except Exception as e:
            logger.error(f"Error loading plugin configuration: {e}")

    def discover_plugins(self) -> List[str]:
        """Discover available plugins in plugin directories."""
        discovered = []

        for plugin_dir in self.plugin_directories:
            if not os.path.exists(plugin_dir):
                continue

            for item in os.listdir(plugin_dir):
                item_path = os.path.join(plugin_dir, item)

                # Check for Python files
                if item.endswith('.py') and not item.startswith('__'):
                    discovered.append(os.path.splitext(item)[0])

                # Check for plugin directories
                elif os.path.isdir(item_path) and os.path.exists(os.path.join(item_path, '__init__.py')):
                    discovered.append(item)

        logger.info(f"Discovered {len(discovered)} plugins: {discovered}")
        return discovered

    def load_plugin(self, plugin_name: str) -> bool:
        """Load a specific plugin."""
        try:
            # Try to import the plugin module
            module = None
            for plugin_dir in self.plugin_directories:
                if plugin_dir not in sys.path:
                    sys.path.append(plugin_dir)

                try:
                    module = importlib.import_module(plugin_name)
                    break
                except ImportError:
                    continue

            if not module:
                logger.error(f"Could not import plugin module: {plugin_name}")
                return False

            # Find plugin class in module
            plugin_class = None
            for name, obj in inspect.getmembers(module):
                if (inspect.isclass(obj) and
                    issubclass(obj, PluginBase) and
                    obj != PluginBase and
                    not inspect.isabstract(obj)):
                    plugin_class = obj
                    break

            if not plugin_class:
                logger.error(f"No valid plugin class found in module: {plugin_name}")
                return False

            # Get plugin configuration
            plugin_config = self.config.get(plugin_name, {})

            # Create plugin instance
            plugin_instance = plugin_class(plugin_config)

            # Validate configuration
            if not plugin_instance.validate_config():
                logger.error(f"Plugin {plugin_name} configuration validation failed")
                return False

            # Initialize plugin
            if not await plugin_instance.initialize():
                logger.error(f"Plugin {plugin_name} initialization failed")
                return False

            # Store plugin
            self.plugin_classes[plugin_name] = plugin_class
            self.loaded_plugins[plugin_name] = plugin_instance

            # Register plugin with appropriate hooks
            self._register_plugin_with_hooks(plugin_instance)

            logger.info(f"Successfully loaded plugin: {plugin_name}")
            return True

        except Exception as e:
            logger.error(f"Error loading plugin {plugin_name}: {e}")
            return False

    def _register_plugin_with_hooks(self, plugin: PluginBase):
        """Register plugin with appropriate hooks based on its base classes."""
        for hook_name, hook in self.hooks.items():
            if isinstance(plugin, hook.plugin_type):
                hook.register_plugin(plugin)

    async def load_all_plugins(self) -> Dict[str, bool]:
        """Load all discovered plugins."""
        discovered = self.discover_plugins()
        results = {}

        for plugin_name in discovered:
            # Check if plugin is enabled in config
            plugin_config = self.config.get(plugin_name, {})
            if not plugin_config.get('enabled', True):
                logger.info(f"Plugin {plugin_name} is disabled, skipping")
                results[plugin_name] = False
                continue

            results[plugin_name] = await self.load_plugin(plugin_name)

        return results

    def unload_plugin(self, plugin_name: str) -> bool:
        """Unload a specific plugin."""
        try:
            if plugin_name not in self.loaded_plugins:
                logger.warning(f"Plugin {plugin_name} is not loaded")
                return False

            plugin = self.loaded_plugins[plugin_name]

            # Cleanup plugin
            plugin.cleanup()

            # Unregister from hooks
            for hook in self.hooks.values():
                hook.unregister_plugin(plugin)

            # Remove from loaded plugins
            del self.loaded_plugins[plugin_name]
            if plugin_name in self.plugin_classes:
                del self.plugin_classes[plugin_name]

            logger.info(f"Successfully unloaded plugin: {plugin_name}")
            return True

        except Exception as e:
            logger.error(f"Error unloading plugin {plugin_name}: {e}")
            return False

    async def unload_all_plugins(self):
        """Unload all loaded plugins."""
        plugin_names = list(self.loaded_plugins.keys())
        for plugin_name in plugin_names:
            await self.unload_plugin(plugin_name)

    def get_loaded_plugins(self) -> Dict[str, PluginMetadata]:
        """Get metadata for all loaded plugins."""
        metadata = {}
        for name, plugin in self.loaded_plugins.items():
            metadata[name] = plugin.get_metadata()
        return metadata

    def get_hook(self, hook_name: str) -> Optional[PluginHook]:
        """Get a specific hook."""
        return self.hooks.get(hook_name)

    async def execute_hook(self, hook_name: str, method_name: str, *args, **kwargs) -> List[Any]:
        """Execute a method on all plugins registered for a hook."""
        hook = self.hooks.get(hook_name)
        if not hook:
            logger.warning(f"Hook {hook_name} not found")
            return []

        return await hook.execute_plugins(method_name, *args, **kwargs)

    def reload_plugin(self, plugin_name: str) -> bool:
        """Reload a specific plugin."""
        if plugin_name in self.loaded_plugins:
            self.unload_plugin(plugin_name)

        return self.load_plugin(plugin_name)

    def get_plugin_status(self) -> Dict[str, Any]:
        """Get status of all plugins."""
        status = {
            'total_plugins': len(self.loaded_plugins),
            'plugins': {},
            'hooks': {}
        }

        # Plugin status
        for name, plugin in self.loaded_plugins.items():
            metadata = plugin.get_metadata()
            status['plugins'][name] = {
                'name': metadata.name,
                'version': metadata.version,
                'enabled': metadata.enabled,
                'description': metadata.description
            }

        # Hook status
        for name, hook in self.hooks.items():
            status['hooks'][name] = {
                'plugin_count': len(hook.plugins),
                'plugins': [p.__class__.__name__ for p in hook.plugins]
            }

        return status

    def create_plugin_template(self, plugin_name: str, plugin_type: str = "data_processor"):
        """Create a template for a new plugin."""
        template_mapping = {
            "data_processor": DataProcessorPlugin,
            "transformer": TransformerPlugin,
            "notification": NotificationPlugin,
            "external_service": ExternalServicePlugin,
            "validation": ValidationPlugin
        }

        base_class = template_mapping.get(plugin_type, DataProcessorPlugin)
        base_class_name = base_class.__name__

        template = f'''#!/usr/bin/env python3
"""
{plugin_name} Plugin

Custom plugin for Gitea-Kimai Integration.
"""

import logging
from typing import Dict, List, Any
from ..plugins import {base_class_name}, PluginMetadata

logger = logging.getLogger(__name__)

class {plugin_name.title()}Plugin({base_class_name}):
    """Custom {plugin_type} plugin."""

    def get_metadata(self) -> PluginMetadata:
        """Return plugin metadata."""
        return PluginMetadata(
            name="{plugin_name}",
            version="1.0.0",
            description="Custom {plugin_type} plugin",
            author="Your Name",
            dependencies=[],
            enabled=True
        )

    async def initialize(self) -> bool:
        """Initialize the plugin."""
        self.logger.info("Initializing {plugin_name} plugin")
        return True

    async def cleanup(self) -> None:
        """Cleanup plugin resources."""
        self.logger.info("Cleaning up {plugin_name} plugin")
'''

        # Add method stubs based on plugin type
        if plugin_type == "data_processor":
            template += '''
    async def process_issue(self, repository: str, issue: Dict[str, Any]) -> Dict[str, Any]:
        """Process issue data before sync."""
        # Add your processing logic here
        return issue

    async def process_pull_request(self, repository: str, pr: Dict[str, Any]) -> Dict[str, Any]:
        """Process pull request data before sync."""
        # Add your processing logic here
        return pr

    async def process_commit(self, repository: str, commit: Dict[str, Any]) -> Dict[str, Any]:
        """Process commit data before sync."""
        # Add your processing logic here
        return commit
'''

        plugin_file = f"plugins/{plugin_name}.py"
        os.makedirs(os.path.dirname(plugin_file), exist_ok=True)

        with open(plugin_file, 'w') as f:
            f.write(template)

        logger.info(f"Created plugin template: {plugin_file}")
        return plugin_file

# Global plugin manager instance
plugin_manager = PluginManager()
