#!/usr/bin/env python3
"""
Main entry point for the Gitea-Kimai Integration System
"""

import sys
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from core.sync_engine import SyncEngine
from config.config_manager import ConfigManager
from utils.logging_enhanced import setup_logging
from utils.system_info import get_system_info
from utils.system_info import get_system_info

def main():
    """Main application entry point."""
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting Gitea-Kimai Integration System")
        
        # Load configuration
        config_manager = ConfigManager()
        config = config_manager.load_config()
        
        # Initialize sync engine
        sync_engine = SyncEngine(config)
        
        # Start the sync process
        sync_engine.start()
        
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Application error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
from utils.constants import API_VERSION
