#!/usr/bin/env python3
"""
GraphQL Schema for Gitea-Kimai Integration

This module provides a GraphQL interface for querying and mutating data
in the Gitea-Kimai sync system, enabling flexible data access and real-time subscriptions.
"""

import graphene
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from graphene import ObjectType, String, Int, Float, Boolean, DateTime, List as GrapheneList
from graphene import Field, Argument, Mutation, Schema, Subscription
import asyncio
import json

logger = logging.getLogger(__name__)

# Scalar Types
class JSONType(graphene.Scalar):
    """Custom scalar type for JSON data."""

    @staticmethod
    def serialize(value):
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return value

    @staticmethod
    def parse_literal(node):
        if isinstance(node.value, str):
            try:
                return json.loads(node.value)
            except:
                return node.value
        return node.value

    @staticmethod
    def parse_value(value):
        if isinstance(value, str):
            try:
                return json.loads(value)
            except:
                return value
        return value

# Object Types
class UserType(ObjectType):
    """GraphQL type for user data."""
    id = String()
    username = String()
    email = String()
    name = String()
    avatar_url = String()
    created_at = DateTime()
    last_login = DateTime()
    is_active = Boolean()

class RepositoryType(ObjectType):
    """GraphQL type for repository data."""
    id = String()
    name = String()
    full_name = String()
    description = String()
    private = Boolean()
    clone_url = String()
    html_url = String()
    created_at = DateTime()
    updated_at = DateTime()
    language = String()
    size = Int()
    stars_count = Int()
    forks_count = Int()
    issues_count = Int()
    open_issues_count = Int()

class IssueType(ObjectType):
    """GraphQL type for issue data."""
    id = String()
    number = Int()
    title = String()
    body = String()
    state = String()
    created_at = DateTime()
    updated_at = DateTime()
    closed_at = DateTime()
    user = Field(UserType)
    assignee = Field(UserType)
    repository = Field(RepositoryType)
    labels = GrapheneList(String)
    milestone = String()
    comments_count = Int()

    def resolve_user(self, info):
        # This would fetch user data from the database
        return UserType(
            id=self.get('user_id'),
            username=self.get('user_login'),
            name=self.get('user_name')
        )

    def resolve_assignee(self, info):
        if self.get('assignee_id'):
            return UserType(
                id=self.get('assignee_id'),
                username=self.get('assignee_login'),
                name=self.get('assignee_name')
            )
        return None

    def resolve_repository(self, info):
        return RepositoryType(
            id=self.get('repository_id'),
            name=self.get('repository_name'),
            full_name=self.get('repository_full_name')
        )

class TimesheetType(ObjectType):
    """GraphQL type for timesheet entries."""
    id = String()
    description = String()
    begin = DateTime()
    end = DateTime()
    duration = Int()
    rate = Float()
    internal_rate = Float()
    user = Field(UserType)
    project = String()
    activity = String()
    tags = GrapheneList(String)
    exported = Boolean()
    billable = Boolean()
    hourly_rate = Float()
    fixed_rate = Float()
    created_at = DateTime()
    updated_at = DateTime()
    meta_fields = JSONType()

    def resolve_user(self, info):
        return UserType(
            id=self.get('user_id'),
            username=self.get('username'),
            name=self.get('user_name')
        )

class ProjectType(ObjectType):
    """GraphQL type for project data."""
    id = String()
    name = String()
    comment = String()
    visible = Boolean()
    budget = Float()
    time_budget = Int()
    customer = String()
    color = String()
    created_at = DateTime()
    updated_at = DateTime()
    timesheets = GrapheneList(TimesheetType)

    def resolve_timesheets(self, info, limit=None):
        # This would fetch timesheets for this project
        return []

class SyncOperationType(ObjectType):
    """GraphQL type for sync operations."""
    id = String()
    sync_type = String()
    source_system = String()
    target_system = String()
    status = String()
    started_at = DateTime()
    completed_at = DateTime()
    items_processed = Int()
    items_synced = Int()
    items_failed = Int()
    conflicts_resolved = Int()
    errors = GrapheneList(String)
    duration = Float()

    def resolve_duration(self, info):
        if self.get('started_at') and self.get('completed_at'):
            start = datetime.fromisoformat(self.get('started_at'))
            end = datetime.fromisoformat(self.get('completed_at'))
            return (end - start).total_seconds()
        return None

class MetricsType(ObjectType):
    """GraphQL type for system metrics."""
    total_syncs = Int()
    successful_syncs = Int()
    failed_syncs = Int()
    success_rate = Float()
    average_duration = Float()
    total_items_synced = Int()
    last_sync_time = DateTime()

class AuditEventType(ObjectType):
    """GraphQL type for audit events."""
    event_id = String()
    timestamp = DateTime()
    event_type = String()
    severity = String()
    outcome = String()
    user_id = String()
    user_ip = String()
    resource = String()
    action = String()
    details = JSONType()
    source_system = String()
    target_system = String()

# Input Types
class IssueFilterInput(graphene.InputObjectType):
    """Input type for filtering issues."""
    repository = String()
    state = String()
    assignee = String()
    labels = GrapheneList(String)
    since = DateTime()
    until = DateTime()

class TimesheetFilterInput(graphene.InputObjectType):
    """Input type for filtering timesheets."""
    user = String()
    project = String()
    activity = String()
    begin = DateTime()
    end = DateTime()
    exported = Boolean()
    billable = Boolean()

class SyncOperationInput(graphene.InputObjectType):
    """Input type for creating sync operations."""
    sync_type = String(required=True)
    repository = String()
    dry_run = Boolean(default_value=False)
    force = Boolean(default_value=False)

# Query Class
class Query(ObjectType):
    """GraphQL queries."""

    # User queries
    users = GrapheneList(UserType, limit=Int(default_value=100), offset=Int(default_value=0))
    user = Field(UserType, id=String(required=True))

    # Repository queries
    repositories = GrapheneList(RepositoryType, limit=Int(default_value=100), offset=Int(default_value=0))
    repository = Field(RepositoryType, id=String(), name=String())

    # Issue queries
    issues = GrapheneList(IssueType, filters=IssueFilterInput(), limit=Int(default_value=100), offset=Int(default_value=0))
    issue = Field(IssueType, id=String(), repository=String(), number=Int())

    # Timesheet queries
    timesheets = GrapheneList(TimesheetType, filters=TimesheetFilterInput(), limit=Int(default_value=100), offset=Int(default_value=0))
    timesheet = Field(TimesheetType, id=String(required=True))

    # Project queries
    projects = GrapheneList(ProjectType, limit=Int(default_value=100), offset=Int(default_value=0))
    project = Field(ProjectType, id=String(), name=String())

    # Sync operation queries
    sync_operations = GrapheneList(SyncOperationType, limit=Int(default_value=100), offset=Int(default_value=0))
    sync_operation = Field(SyncOperationType, id=String(required=True))

    # Metrics queries
    metrics = Field(MetricsType, period_hours=Int(default_value=24))

    # Audit queries
    audit_events = GrapheneList(AuditEventType, limit=Int(default_value=100), offset=Int(default_value=0))

    def resolve_users(self, info, limit=100, offset=0):
        """Resolve users query."""
        # This would fetch users from database
        return []

    def resolve_user(self, info, id):
        """Resolve single user query."""
        # This would fetch user by ID from database
        return UserType(id=id, username=f"user_{id}")

    def resolve_repositories(self, info, limit=100, offset=0):
        """Resolve repositories query."""
        # This would fetch repositories from Gitea API
        return []

    def resolve_repository(self, info, id=None, name=None):
        """Resolve single repository query."""
        if id:
            # Fetch by ID
            return RepositoryType(id=id, name=f"repo_{id}")
        elif name:
            # Fetch by name
            return RepositoryType(name=name, full_name=f"owner/{name}")
        return None

    def resolve_issues(self, info, filters=None, limit=100, offset=0):
        """Resolve issues query."""
        # This would fetch issues from database with filters
        return []

    def resolve_issue(self, info, id=None, repository=None, number=None):
        """Resolve single issue query."""
        if id:
            return IssueType(id=id, title=f"Issue {id}")
        elif repository and number:
            return IssueType(number=number, title=f"Issue #{number}")
        return None

    def resolve_timesheets(self, info, filters=None, limit=100, offset=0):
        """Resolve timesheets query."""
        # This would fetch timesheets from Kimai API
        return []

    def resolve_timesheet(self, info, id):
        """Resolve single timesheet query."""
        return TimesheetType(id=id, description=f"Timesheet {id}")

    def resolve_projects(self, info, limit=100, offset=0):
        """Resolve projects query."""
        # This would fetch projects from Kimai API
        return []

    def resolve_project(self, info, id=None, name=None):
        """Resolve single project query."""
        if id:
            return ProjectType(id=id, name=f"Project {id}")
        elif name:
            return ProjectType(name=name)
        return None

    def resolve_sync_operations(self, info, limit=100, offset=0):
        """Resolve sync operations query."""
        # This would fetch sync operations from database
        return []

    def resolve_sync_operation(self, info, id):
        """Resolve single sync operation query."""
        return SyncOperationType(id=id, status="completed")

    def resolve_metrics(self, info, period_hours=24):
        """Resolve metrics query."""
        # This would calculate metrics from database
        return MetricsType(
            total_syncs=100,
            successful_syncs=95,
            failed_syncs=5,
            success_rate=95.0,
            average_duration=30.5,
            total_items_synced=1500,
            last_sync_time=datetime.now()
        )

    def resolve_audit_events(self, info, limit=100, offset=0):
        """Resolve audit events query."""
        # This would fetch audit events from database
        return []

# Mutations
class TriggerSyncMutation(Mutation):
    """Mutation to trigger a sync operation."""

    class Arguments:
        input = SyncOperationInput(required=True)

    operation = Field(SyncOperationType)
    success = Boolean()
    message = String()

    def mutate(self, info, input):
        """Execute sync trigger mutation."""
        try:
            # This would trigger actual sync operation
            operation_id = f"sync_{int(datetime.now().timestamp())}"

            # Create sync operation record
            operation = SyncOperationType(
                id=operation_id,
                sync_type=input.sync_type,
                source_system="gitea",
                target_system="kimai",
                status="started",
                started_at=datetime.now()
            )

            return TriggerSyncMutation(
                operation=operation,
                success=True,
                message=f"Sync operation {operation_id} started successfully"
            )

        except Exception as e:
            return TriggerSyncMutation(
                operation=None,
                success=False,
                message=f"Failed to start sync operation: {str(e)}"
            )

class UpdateIssueMutation(Mutation):
    """Mutation to update an issue."""

    class Arguments:
        id = String(required=True)
        title = String()
        body = String()
        state = String()
        assignee = String()
        labels = GrapheneList(String)

    issue = Field(IssueType)
    success = Boolean()
    message = String()

    def mutate(self, info, id, **kwargs):
        """Execute issue update mutation."""
        try:
            # This would update the issue in Gitea
            updated_issue = IssueType(
                id=id,
                title=kwargs.get('title', f"Updated Issue {id}"),
                body=kwargs.get('body'),
                state=kwargs.get('state', 'open'),
                updated_at=datetime.now()
            )

            return UpdateIssueMutation(
                issue=updated_issue,
                success=True,
                message=f"Issue {id} updated successfully"
            )

        except Exception as e:
            return UpdateIssueMutation(
                issue=None,
                success=False,
                message=f"Failed to update issue: {str(e)}"
            )

class CreateTimesheetMutation(Mutation):
    """Mutation to create a timesheet entry."""

    class Arguments:
        description = String(required=True)
        project = String(required=True)
        activity = String(required=True)
        begin = DateTime(required=True)
        end = DateTime()
        duration = Int()

    timesheet = Field(TimesheetType)
    success = Boolean()
    message = String()

    def mutate(self, info, description, project, activity, begin, **kwargs):
        """Execute timesheet creation mutation."""
        try:
            # This would create timesheet entry in Kimai
            timesheet_id = f"timesheet_{int(datetime.now().timestamp())}"

            timesheet = TimesheetType(
                id=timesheet_id,
                description=description,
                project=project,
                activity=activity,
                begin=begin,
                end=kwargs.get('end'),
                duration=kwargs.get('duration', 0),
                created_at=datetime.now()
            )

            return CreateTimesheetMutation(
                timesheet=timesheet,
                success=True,
                message=f"Timesheet {timesheet_id} created successfully"
            )

        except Exception as e:
            return CreateTimesheetMutation(
                timesheet=None,
                success=False,
                message=f"Failed to create timesheet: {str(e)}"
            )

class Mutation(ObjectType):
    """GraphQL mutations."""
    trigger_sync = TriggerSyncMutation.Field()
    update_issue = UpdateIssueMutation.Field()
    create_timesheet = CreateTimesheetMutation.Field()

# Subscriptions
class SyncEventSubscription(ObjectType):
    """Subscription for sync events."""

    sync_status_update = Field(SyncOperationType, operation_id=String())

    async def resolve_sync_status_update(self, info, operation_id=None):
        """Resolve sync status updates subscription."""
        # This would yield sync status updates in real-time
        while True:
            # Simulate status update
            yield SyncOperationType(
                id=operation_id or "default",
                status="running",
                items_processed=100,
                started_at=datetime.now()
            )
            await asyncio.sleep(5)

class IssueEventSubscription(ObjectType):
    """Subscription for issue events."""

    issue_updates = Field(IssueType, repository=String())

    async def resolve_issue_updates(self, info, repository=None):
        """Resolve issue updates subscription."""
        # This would yield issue updates in real-time
        while True:
            yield IssueType(
                id=f"issue_{int(datetime.now().timestamp())}",
                title="New Issue",
                state="open",
                created_at=datetime.now()
            )
            await asyncio.sleep(10)

class Subscription(ObjectType):
    """GraphQL subscriptions."""
    sync_status_update = Field(SyncOperationType, operation_id=String())
    issue_updates = Field(IssueType, repository=String())

# Schema
schema = Schema(
    query=Query,
    mutation=Mutation,
    subscription=Subscription
)

# Helper functions
def create_context(request=None):
    """Create GraphQL context with user information and database connections."""
    return {
        'request': request,
        'user': getattr(request, 'user', None),
        'database': None,  # Would be database connection
        'gitea_client': None,  # Would be Gitea API client
        'kimai_client': None,  # Would be Kimai API client
    }

def format_error(error):
    """Format GraphQL errors for response."""
    logger.error(f"GraphQL error: {error}")
    return {
        'message': str(error),
        'type': error.__class__.__name__,
        'path': getattr(error, 'path', None),
        'locations': getattr(error, 'locations', None)
    }

# Example usage and utilities
def get_schema():
    """Get the GraphQL schema."""
    return schema

def execute_query(query_string, variables=None, context=None):
    """Execute a GraphQL query."""
    try:
        result = schema.execute(
            query_string,
            variables=variables,
            context_value=context or {},
            middleware=[]
        )

        response = {'data': result.data}

        if result.errors:
            response['errors'] = [format_error(error) for error in result.errors]

        return response

    except Exception as e:
        logger.error(f"Query execution error: {e}")
        return {
            'data': None,
            'errors': [{'message': str(e), 'type': 'ExecutionError'}]
        }

# Example queries for testing
EXAMPLE_QUERIES = {
    'get_repositories': '''
        query GetRepositories($limit: Int) {
            repositories(limit: $limit) {
                id
                name
                description
                private
                createdAt
                language
            }
        }
    ''',

    'get_issues': '''
        query GetIssues($filters: IssueFilterInput, $limit: Int) {
            issues(filters: $filters, limit: $limit) {
                id
                number
                title
                state
                createdAt
                user {
                    username
                }
                repository {
                    name
                }
            }
        }
    ''',

    'trigger_sync': '''
        mutation TriggerSync($input: SyncOperationInput!) {
            triggerSync(input: $input) {
                success
                message
                operation {
                    id
                    status
                    startedAt
                }
            }
        }
    ''',

    'get_metrics': '''
        query GetMetrics($periodHours: Int) {
            metrics(periodHours: $periodHours) {
                totalSyncs
                successfulSyncs
                successRate
                averageDuration
                lastSyncTime
            }
        }
    '''
}
