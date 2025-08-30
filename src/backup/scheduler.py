#!/usr/bin/env python3
"""
Backup Scheduler Module

Automated backup scheduling with multiple scheduling options,
retry mechanisms, and failure handling.
"""

import os
import time
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
import schedule
import croniter

logger = logging.getLogger(__name__)

@dataclass
class ScheduleConfig:
    """Configuration for backup scheduling."""
    enabled: bool = True
    interval: str = "daily"  # hourly, daily, weekly, monthly, custom
    time: str = "02:00"  # HH:MM format
    day_of_week: str = "sunday"  # for weekly schedules
    day_of_month: int = 1  # for monthly schedules
    cron_expression: str = ""  # for custom cron schedules
    retry_on_failure: bool = True
    max_retries: int = 3
    retry_delay_minutes: int = 30
    parallel_backups: bool = False
    max_parallel_backups: int = 1

@dataclass
class ScheduledTask:
    """Information about a scheduled task."""
    task_id: str
    schedule_config: ScheduleConfig
    callback: Callable
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    run_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    last_error: Optional[str] = None
    is_running: bool = False

class BackupScheduler:
    """Scheduler for automated backup operations."""
    
    def __init__(self):
        self.tasks: Dict[str, ScheduledTask] = {}
        self.scheduler_thread = None
        self.running = False
        self.lock = threading.RLock()
    
    def add_backup_task(self, task_id: str, config: ScheduleConfig, 
                       callback: Callable) -> bool:
        """Add a new backup task to the scheduler."""
        try:
            with self.lock:
                if task_id in self.tasks:
                    logger.warning(f"Task {task_id} already exists, replacing")
                
                task = ScheduledTask(
                    task_id=task_id,
                    schedule_config=config,
                    callback=callback
                )
                
                self._schedule_task(task)
                self.tasks[task_id] = task
                
                logger.info(f"Added backup task: {task_id} - {config.interval}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to add backup task {task_id}: {e}")
            return False
    
    def _schedule_task(self, task: ScheduledTask):
        """Schedule a task based on its configuration."""
        config = task.schedule_config
        
        if config.interval == "hourly":
            schedule.every().hour.do(self._execute_task, task)
        elif config.interval == "daily":
            schedule.every().day.at(config.time).do(self._execute_task, task)
        elif config.interval == "weekly":
            day_method = getattr(schedule.every(), config.day_of_week)
            day_method.at(config.time).do(self._execute_task, task)
        elif config.interval == "monthly":
            # Schedule for the first day of each month
            schedule.every().month.at(config.time).do(self._execute_task, task)
        elif config.interval == "custom" and config.cron_expression:
            self._schedule_cron_task(task, config.cron_expression)
        else:
            raise ValueError(f"Invalid schedule configuration: {config.interval}")
    
    def _schedule_cron_task(self, task: ScheduledTask, cron_expression: str):
        """Schedule a task using cron expression."""
        try:
            cron = croniter.croniter(cron_expression, datetime.now())
            next_run = cron.get_next(datetime)
            
            # Calculate delay until next run
            delay = (next_run - datetime.now()).total_seconds()
            if delay > 0:
                threading.Timer(delay, self._execute_cron_task, args=[task, cron_expression]).start()
            
        except Exception as e:
            logger.error(f"Failed to schedule cron task: {e}")
    
    def _execute_cron_task(self, task: ScheduledTask, cron_expression: str):
        """Execute a cron-based task and schedule the next run."""
        self._execute_task(task)
        
        # Schedule next run
        try:
            cron = croniter.croniter(cron_expression, datetime.now())
            next_run = cron.get_next(datetime)
            delay = (next_run - datetime.now()).total_seconds()
            if delay > 0:
                threading.Timer(delay, self._execute_cron_task, args=[task, cron_expression]).start()
        except Exception as e:
            logger.error(f"Failed to schedule next cron run: {e}")
    
    def _execute_task(self, task: ScheduledTask):
        """Execute a scheduled task with retry logic."""
        if task.is_running:
            logger.warning(f"Task {task.task_id} is already running, skipping")
            return
        
        task.is_running = True
        task.last_run = datetime.now()
        task.run_count += 1
        
        try:
            logger.info(f"Executing scheduled backup task: {task.task_id}")
            
            # Execute the callback
            result = task.callback()
            
            if result:
                task.success_count += 1
                task.last_error = None
                logger.info(f"Backup task {task.task_id} completed successfully")
            else:
                task.failure_count += 1
                task.last_error = "Backup callback returned False"
                logger.error(f"Backup task {task.task_id} failed")
                
                # Retry on failure if configured
                if task.schedule_config.retry_on_failure:
                    self._schedule_retry(task)
                    
        except Exception as e:
            task.failure_count += 1
            task.last_error = str(e)
            logger.error(f"Backup task {task.task_id} failed with exception: {e}")
            
            # Retry on failure if configured
            if task.schedule_config.retry_on_failure:
                self._schedule_retry(task)
        
        finally:
            task.is_running = False
            task.next_run = self._calculate_next_run(task)
    
    def _schedule_retry(self, task: ScheduledTask):
        """Schedule a retry for a failed task."""
        config = task.schedule_config
        
        if task.failure_count < config.max_retries:
            retry_delay = config.retry_delay_minutes * 60  # Convert to seconds
            logger.info(f"Scheduling retry for task {task.task_id} in {config.retry_delay_minutes} minutes")
            
            threading.Timer(retry_delay, self._execute_task, args=[task]).start()
        else:
            logger.error(f"Task {task.task_id} exceeded maximum retries ({config.max_retries})")
    
    def _calculate_next_run(self, task: ScheduledTask) -> Optional[datetime]:
        """Calculate the next run time for a task."""
        config = task.schedule_config
        
        if config.interval == "hourly":
            return datetime.now() + timedelta(hours=1)
        elif config.interval == "daily":
            next_run = datetime.now().replace(
                hour=int(config.time.split(':')[0]),
                minute=int(config.time.split(':')[1]),
                second=0,
                microsecond=0
            )
            if next_run <= datetime.now():
                next_run += timedelta(days=1)
            return next_run
        elif config.interval == "weekly":
            # Calculate next occurrence of the specified day
            days_ahead = self._get_days_ahead(config.day_of_week)
            next_run = datetime.now() + timedelta(days=days_ahead)
            return next_run.replace(
                hour=int(config.time.split(':')[0]),
                minute=int(config.time.split(':')[1]),
                second=0,
                microsecond=0
            )
        elif config.interval == "monthly":
            # Calculate next month
            next_run = datetime.now().replace(day=config.day_of_month)
            if next_run <= datetime.now():
                # Move to next month
                if next_run.month == 12:
                    next_run = next_run.replace(year=next_run.year + 1, month=1)
                else:
                    next_run = next_run.replace(month=next_run.month + 1)
            return next_run.replace(
                hour=int(config.time.split(':')[0]),
                minute=int(config.time.split(':')[1]),
                second=0,
                microsecond=0
            )
        
        return None
    
    def _get_days_ahead(self, day_name: str) -> int:
        """Calculate days ahead for weekly schedule."""
        day_map = {
            'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
            'friday': 4, 'saturday': 5, 'sunday': 6
        }
        
        target_day = day_map.get(day_name.lower(), 0)
        current_day = datetime.now().weekday()
        
        days_ahead = target_day - current_day
        if days_ahead <= 0:
            days_ahead += 7
        
        return days_ahead
    
    def remove_task(self, task_id: str) -> bool:
        """Remove a scheduled task."""
        try:
            with self.lock:
                if task_id in self.tasks:
                    # Clear the task from schedule
                    schedule.clear(task_id)
                    del self.tasks[task_id]
                    logger.info(f"Removed backup task: {task_id}")
                    return True
                else:
                    logger.warning(f"Task {task_id} not found")
                    return False
        except Exception as e:
            logger.error(f"Failed to remove task {task_id}: {e}")
            return False
    
    def get_task_info(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a scheduled task."""
        if task_id in self.tasks:
            task = self.tasks[task_id]
            return {
                'task_id': task.task_id,
                'enabled': task.schedule_config.enabled,
                'interval': task.schedule_config.interval,
                'time': task.schedule_config.time,
                'last_run': task.last_run.isoformat() if task.last_run else None,
                'next_run': task.next_run.isoformat() if task.next_run else None,
                'run_count': task.run_count,
                'success_count': task.success_count,
                'failure_count': task.failure_count,
                'last_error': task.last_error,
                'is_running': task.is_running
            }
        return None
    
    def get_all_tasks(self) -> List[Dict[str, Any]]:
        """Get information about all scheduled tasks."""
        return [self.get_task_info(task_id) for task_id in self.tasks.keys()]
    
    def start(self):
        """Start the scheduler."""
        if self.running:
            logger.warning("Scheduler is already running")
            return
        
        self.running = True
        self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.scheduler_thread.start()
        logger.info("Backup scheduler started")
    
    def stop(self):
        """Stop the scheduler."""
        self.running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        logger.info("Backup scheduler stopped")
    
    def _run_scheduler(self):
        """Main scheduler loop."""
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                time.sleep(5)
    
    def trigger_task(self, task_id: str) -> bool:
        """Manually trigger a scheduled task."""
        if task_id in self.tasks:
            task = self.tasks[task_id]
            if not task.is_running:
                threading.Thread(target=self._execute_task, args=[task], daemon=True).start()
                logger.info(f"Manually triggered task: {task_id}")
                return True
            else:
                logger.warning(f"Task {task_id} is already running")
                return False
        else:
            logger.warning(f"Task {task_id} not found")
            return False

def create_scheduler() -> BackupScheduler:
    """Create and return a backup scheduler instance."""
    return BackupScheduler()
