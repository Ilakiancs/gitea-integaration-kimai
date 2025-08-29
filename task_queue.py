#!/usr/bin/env python3
"""
Task Queue Module

Provides a task queue system for handling background jobs, asynchronous
processing, and job scheduling for the sync system.
"""

import time
import json
import logging
import threading
import queue
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
import sqlite3
import uuid
import pickle

logger = logging.getLogger(__name__)

class TaskStatus(Enum):
    """Task status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRY = "retry"

class TaskPriority(Enum):
    """Task priority enumeration."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4

@dataclass
class Task:
    """Represents a task in the queue."""
    id: str
    name: str
    func_name: str
    args: tuple
    kwargs: dict
    priority: TaskPriority
    status: TaskStatus
    created_at: datetime
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    retry_delay: int = 60  # seconds
    tags: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None

class TaskQueue:
    """Main task queue implementation."""
    
    def __init__(self, db_path: str = "task_queue.db", max_workers: int = 4):
        self.db_path = db_path
        self.max_workers = max_workers
        self.task_queue = queue.PriorityQueue()
        self.workers: List[TaskWorker] = []
        self.running = False
        self.registered_tasks: Dict[str, Callable] = {}
        self.lock = threading.RLock()
        self._init_database()
    
    def _init_database(self):
        """Initialize the task queue database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    func_name TEXT NOT NULL,
                    args BLOB,
                    kwargs BLOB,
                    priority INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at DATETIME NOT NULL,
                    scheduled_at DATETIME,
                    started_at DATETIME,
                    completed_at DATETIME,
                    result BLOB,
                    error TEXT,
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    retry_delay INTEGER DEFAULT 60,
                    tags TEXT,
                    metadata TEXT
                )
            """)
            
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_priority ON tasks(priority)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_scheduled ON tasks(scheduled_at)")
            conn.commit()
    
    def register_task(self, name: str, func: Callable):
        """Register a task function."""
        self.registered_tasks[name] = func
        logger.info(f"Registered task: {name}")
    
    def enqueue(self, name: str, *args, priority: TaskPriority = TaskPriority.NORMAL,
                scheduled_at: Optional[datetime] = None, max_retries: int = 3,
                retry_delay: int = 60, tags: Optional[List[str]] = None,
                metadata: Optional[Dict[str, Any]] = None, **kwargs) -> str:
        """Add a task to the queue."""
        if name not in self.registered_tasks:
            raise ValueError(f"Task '{name}' is not registered")
        
        task_id = str(uuid.uuid4())
        task = Task(
            id=task_id,
            name=name,
            func_name=name,
            args=args,
            kwargs=kwargs,
            priority=priority,
            status=TaskStatus.PENDING,
            created_at=datetime.now(),
            scheduled_at=scheduled_at,
            max_retries=max_retries,
            retry_delay=retry_delay,
            tags=tags or [],
            metadata=metadata or {}
        )
        
        # Store in database
        self._store_task(task)
        
        # Add to queue if not scheduled for future
        if scheduled_at is None or scheduled_at <= datetime.now():
            self._add_to_queue(task)
        
        logger.info(f"Enqueued task {task_id}: {name}")
        return task_id
    
    def _store_task(self, task: Task):
        """Store task in database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO tasks (
                    id, name, func_name, args, kwargs, priority, status, created_at,
                    scheduled_at, max_retries, retry_delay, tags, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                task.id, task.name, task.func_name,
                pickle.dumps(task.args), pickle.dumps(task.kwargs),
                task.priority.value, task.status.value, task.created_at.isoformat(),
                task.scheduled_at.isoformat() if task.scheduled_at else None,
                task.max_retries, task.retry_delay,
                json.dumps(task.tags), json.dumps(task.metadata)
            ))
            conn.commit()
    
    def _add_to_queue(self, task: Task):
        """Add task to the priority queue."""
        # Priority queue uses negative priority for higher priority first
        priority = -task.priority.value
        self.task_queue.put((priority, task.created_at.timestamp(), task))
    
    def get_task(self, task_id: str) -> Optional[Task]:
        """Get task by ID."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            return self._row_to_task(row)
    
    def _row_to_task(self, row) -> Task:
        """Convert database row to Task object."""
        return Task(
            id=row[0],
            name=row[1],
            func_name=row[2],
            args=pickle.loads(row[3]) if row[3] else (),
            kwargs=pickle.loads(row[4]) if row[4] else {},
            priority=TaskPriority(row[5]),
            status=TaskStatus(row[6]),
            created_at=datetime.fromisoformat(row[7]),
            scheduled_at=datetime.fromisoformat(row[8]) if row[8] else None,
            started_at=datetime.fromisoformat(row[9]) if row[9] else None,
            completed_at=datetime.fromisoformat(row[10]) if row[10] else None,
            result=pickle.loads(row[11]) if row[11] else None,
            error=row[12],
            retry_count=row[13],
            max_retries=row[14],
            retry_delay=row[15],
            tags=json.loads(row[16]) if row[16] else [],
            metadata=json.loads(row[17]) if row[17] else {}
        )
    
    def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending task."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                UPDATE tasks SET status = ? WHERE id = ? AND status = ?
            """, (TaskStatus.CANCELLED.value, task_id, TaskStatus.PENDING.value))
            conn.commit()
            
            if cursor.rowcount > 0:
                logger.info(f"Cancelled task: {task_id}")
                return True
            return False
    
    def retry_task(self, task_id: str) -> bool:
        """Retry a failed task."""
        task = self.get_task(task_id)
        if not task or task.status != TaskStatus.FAILED:
            return False
        
        if task.retry_count >= task.max_retries:
            logger.warning(f"Task {task_id} has exceeded max retries")
            return False
        
        # Reset task for retry
        task.status = TaskStatus.PENDING
        task.retry_count += 1
        task.error = None
        task.started_at = None
        task.completed_at = None
        task.result = None
        
        # Update database
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE tasks SET 
                    status = ?, retry_count = ?, error = NULL, 
                    started_at = NULL, completed_at = NULL, result = NULL
                WHERE id = ?
            """, (task.status.value, task.retry_count, task_id))
            conn.commit()
        
        # Add back to queue
        self._add_to_queue(task)
        logger.info(f"Retrying task: {task_id}")
        return True
    
    def get_pending_tasks(self) -> List[Task]:
        """Get all pending tasks."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT * FROM tasks 
                WHERE status = ? 
                ORDER BY priority DESC, created_at ASC
            """, (TaskStatus.PENDING.value,))
            
            return [self._row_to_task(row) for row in cursor.fetchall()]
    
    def get_failed_tasks(self) -> List[Task]:
        """Get all failed tasks."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT * FROM tasks 
                WHERE status = ? 
                ORDER BY created_at DESC
            """, (TaskStatus.FAILED.value,))
            
            return [self._row_to_task(row) for row in cursor.fetchall()]
    
    def get_task_stats(self) -> Dict[str, Any]:
        """Get task queue statistics."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT status, COUNT(*) FROM tasks GROUP BY status
            """)
            status_counts = dict(cursor.fetchall())
            
            cursor = conn.execute("SELECT COUNT(*) FROM tasks WHERE status = ?", (TaskStatus.RUNNING.value,))
            running_count = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT COUNT(*) FROM tasks WHERE status = ?", (TaskStatus.PENDING.value,))
            pending_count = cursor.fetchone()[0]
        
        return {
            'total_tasks': sum(status_counts.values()),
            'pending': status_counts.get(TaskStatus.PENDING.value, 0),
            'running': status_counts.get(TaskStatus.RUNNING.value, 0),
            'completed': status_counts.get(TaskStatus.COMPLETED.value, 0),
            'failed': status_counts.get(TaskStatus.FAILED.value, 0),
            'cancelled': status_counts.get(TaskStatus.CANCELLED.value, 0),
            'queue_size': self.task_queue.qsize(),
            'active_workers': len([w for w in self.workers if w.is_alive()])
        }
    
    def start(self):
        """Start the task queue workers."""
        if self.running:
            logger.warning("Task queue is already running")
            return
        
        self.running = True
        
        # Start workers
        for i in range(self.max_workers):
            worker = TaskWorker(self, f"Worker-{i+1}")
            worker.start()
            self.workers.append(worker)
        
        # Start scheduler for delayed tasks
        self.scheduler = TaskScheduler(self)
        self.scheduler.start()
        
        logger.info(f"Started task queue with {self.max_workers} workers")
    
    def stop(self):
        """Stop the task queue workers."""
        if not self.running:
            return
        
        self.running = False
        
        # Stop workers
        for worker in self.workers:
            worker.stop()
        
        # Stop scheduler
        if hasattr(self, 'scheduler'):
            self.scheduler.stop()
        
        logger.info("Stopped task queue")
    
    def wait_for_completion(self, timeout: Optional[float] = None):
        """Wait for all tasks to complete."""
        start_time = time.time()
        
        while self.running:
            stats = self.get_task_stats()
            if stats['pending'] == 0 and stats['running'] == 0:
                break
            
            if timeout and (time.time() - start_time) > timeout:
                logger.warning("Timeout waiting for task completion")
                break
            
            time.sleep(1)

class TaskWorker(threading.Thread):
    """Worker thread for processing tasks."""
    
    def __init__(self, task_queue: TaskQueue, name: str):
        super().__init__(name=name, daemon=True)
        self.task_queue = task_queue
        self.running = False
    
    def run(self):
        """Main worker loop."""
        self.running = True
        
        while self.running:
            try:
                # Get task from queue with timeout
                priority, timestamp, task = self.task_queue.get(timeout=1)
                
                # Check if task is still valid
                current_task = self.task_queue.get_task(task.id)
                if not current_task or current_task.status != TaskStatus.PENDING:
                    self.task_queue.task_queue.task_done()
                    continue
                
                # Process task
                self._process_task(task)
                
                self.task_queue.task_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Worker {self.name} encountered error: {e}")
    
    def _process_task(self, task: Task):
        """Process a single task."""
        logger.info(f"Processing task {task.id}: {task.name}")
        
        # Update status to running
        self._update_task_status(task.id, TaskStatus.RUNNING, started_at=datetime.now())
        
        try:
            # Get the task function
            func = self.task_queue.registered_tasks.get(task.func_name)
            if not func:
                raise ValueError(f"Task function '{task.func_name}' not found")
            
            # Execute task
            result = func(*task.args, **task.kwargs)
            
            # Update status to completed
            self._update_task_status(task.id, TaskStatus.COMPLETED, 
                                   completed_at=datetime.now(), result=result)
            
            logger.info(f"Completed task {task.id}: {task.name}")
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Task {task.id} failed: {error_msg}")
            
            # Check if task should be retried
            if task.retry_count < task.max_retries:
                # Schedule retry
                retry_delay = task.retry_delay * (2 ** task.retry_count)  # Exponential backoff
                retry_time = datetime.now() + timedelta(seconds=retry_delay)
                
                self._update_task_status(task.id, TaskStatus.RETRY, 
                                       scheduled_at=retry_time, error=error_msg)
                
                # Add to scheduler for retry
                if hasattr(self.task_queue, 'scheduler'):
                    self.task_queue.scheduler.schedule_task(task, retry_time)
            else:
                # Mark as failed
                self._update_task_status(task.id, TaskStatus.FAILED, error=error_msg)
    
    def _update_task_status(self, task_id: str, status: TaskStatus, **kwargs):
        """Update task status in database."""
        with sqlite3.connect(self.task_queue.db_path) as conn:
            update_fields = ["status = ?"]
            params = [status.value]
            
            if 'started_at' in kwargs:
                update_fields.append("started_at = ?")
                params.append(kwargs['started_at'].isoformat())
            
            if 'completed_at' in kwargs:
                update_fields.append("completed_at = ?")
                params.append(kwargs['completed_at'].isoformat())
            
            if 'result' in kwargs:
                update_fields.append("result = ?")
                params.append(pickle.dumps(kwargs['result']))
            
            if 'error' in kwargs:
                update_fields.append("error = ?")
                params.append(kwargs['error'])
            
            if 'scheduled_at' in kwargs:
                update_fields.append("scheduled_at = ?")
                params.append(kwargs['scheduled_at'].isoformat())
            
            params.append(task_id)
            
            conn.execute(f"UPDATE tasks SET {', '.join(update_fields)} WHERE id = ?", params)
            conn.commit()
    
    def stop(self):
        """Stop the worker."""
        self.running = False

class TaskScheduler(threading.Thread):
    """Scheduler for delayed tasks."""
    
    def __init__(self, task_queue: TaskQueue):
        super().__init__(name="TaskScheduler", daemon=True)
        self.task_queue = task_queue
        self.running = False
        self.scheduled_tasks: Dict[str, datetime] = {}
    
    def run(self):
        """Main scheduler loop."""
        self.running = True
        
        while self.running:
            try:
                current_time = datetime.now()
                
                # Check for tasks that should be executed
                tasks_to_execute = []
                for task_id, scheduled_time in list(self.scheduled_tasks.items()):
                    if scheduled_time <= current_time:
                        tasks_to_execute.append(task_id)
                        del self.scheduled_tasks[task_id]
                
                # Execute scheduled tasks
                for task_id in tasks_to_execute:
                    task = self.task_queue.get_task(task_id)
                    if task and task.status == TaskStatus.RETRY:
                        # Reset status to pending and add to queue
                        with sqlite3.connect(self.task_queue.db_path) as conn:
                            conn.execute("UPDATE tasks SET status = ? WHERE id = ?", 
                                       (TaskStatus.PENDING.value, task_id))
                            conn.commit()
                        
                        self.task_queue._add_to_queue(task)
                        logger.info(f"Scheduled retry for task: {task_id}")
                
                # Check for new scheduled tasks
                self._load_scheduled_tasks()
                
                time.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                time.sleep(10)
    
    def _load_scheduled_tasks(self):
        """Load scheduled tasks from database."""
        with sqlite3.connect(self.task_queue.db_path) as conn:
            cursor = conn.execute("""
                SELECT id, scheduled_at FROM tasks 
                WHERE status = ? AND scheduled_at IS NOT NULL
            """, (TaskStatus.RETRY.value,))
            
            for row in cursor.fetchall():
                task_id, scheduled_at = row
                if task_id not in self.scheduled_tasks:
                    self.scheduled_tasks[task_id] = datetime.fromisoformat(scheduled_at)
    
    def schedule_task(self, task: Task, scheduled_time: datetime):
        """Schedule a task for later execution."""
        self.scheduled_tasks[task.id] = scheduled_time
    
    def stop(self):
        """Stop the scheduler."""
        self.running = False

# Predefined task functions for the sync system
def sync_repository_task(repository: str, force: bool = False):
    """Task function for syncing a repository."""
    logger.info(f"Starting sync for repository: {repository}")
    
    # Simulate sync work
    time.sleep(2)
    
    # Simulate potential failure
    if repository == "problematic-repo" and not force:
        raise Exception("Repository sync failed")
    
    logger.info(f"Completed sync for repository: {repository}")
    return {"repository": repository, "synced_items": 42}

def cleanup_old_data_task(days: int = 30):
    """Task function for cleaning up old data."""
    logger.info(f"Starting cleanup of data older than {days} days")
    
    # Simulate cleanup work
    time.sleep(1)
    
    logger.info(f"Completed cleanup of old data")
    return {"cleaned_records": 150}

def generate_report_task(report_type: str, output_format: str = "csv"):
    """Task function for generating reports."""
    logger.info(f"Generating {report_type} report in {output_format} format")
    
    # Simulate report generation
    time.sleep(3)
    
    logger.info(f"Completed {report_type} report generation")
    return {"report_type": report_type, "format": output_format, "generated": True}

def backup_database_task(backup_path: str):
    """Task function for database backup."""
    logger.info(f"Starting database backup to {backup_path}")
    
    # Simulate backup work
    time.sleep(5)
    
    logger.info(f"Completed database backup")
    return {"backup_path": backup_path, "backup_size": "15.2 MB"}
