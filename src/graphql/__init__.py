#!/usr/bin/env python3
"""
GraphQL API Interface for Gitea-Kimai Integration

Provides a modern GraphQL API for querying and mutating sync data,
offering flexible data fetching and real-time subscriptions.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
import strawberry
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info
from strawberry.subscriptions import GRAPHQL_TRANSPORT_WS_PROTOCOL, GRAPHQL_WS_PROTOCOL
import asyncpg
from dataclasses import dataclass

from ..core.async_sync import AsyncGiteaKimaiSync
from ..multitenant import tenant_manager, TenantType, TenantStatus
from ..security.security import SecurityManager, Role, Permission
from ..monitoring.metrics import MetricsCollector
from ..config.config_manager import ConfigurationManager

logger = logging.getLogger(__name__)

# GraphQL Types
@strawberry.type
class Repository:
    """Repository information."""
    name: str
    organization: str
    description: Optional[str] = None
    url: str
    last_sync: Optional[datetime] = None
    issue_count: int = 0
    pr_count: int = 0
    sync_enabled: bool = True

@strawberry.type
class Issue:
    """Issue information."""
    id: int
    number: int
    title: str
    body: Optional[str] = None
    state: str
    author: str
    assignees: List[str]
    labels: List[str]
    created_at: datetime
    updated_at: Optional[datetime] = None
    repository: str
    kimai_activity_id: Optional[int] = None
    sync_status: str = "pending"

@strawberry.type
class PullRequest:
    """Pull request information."""
    id: int
    number: int
    title: str
    body: Optional[str] = None
    state: str
    author: str
    base_branch: str
    head_branch: str
    created_at: datetime
    updated_at: Optional[datetime] = None
    merged_at: Optional[datetime] = None
    repository: str
    kimai_activity_id: Optional[int] = None
    sync_status: str = "pending"

@strawberry.type
class KimaiProject:
    """Kimai project information."""
    id: int
    name: str
    comment: Optional[str] = None
    visible: bool = True
    budget: Optional[float] = None
    time_budget: Optional[int] = None
    created_at: datetime
    activities: List["KimaiActivity"]

@strawberry.type
class KimaiActivity:
    """Kimai activity information."""
    id: int
    name: str
    comment: Optional[str] = None
    project_id: int
    visible: bool = True
    budget: Optional[float] = None
    time_budget: Optional[int] = None
    created_at: datetime

@strawberry.type
class SyncResult:
    """Synchronization result."""
    id: str
    repository: str
    task_type: str
    success: bool
    items_processed: int
    items_created: int
    items_updated: int
    duration: float
    error_message: Optional[str] = None
    timestamp: datetime

@strawberry.type
class Tenant:
    """Tenant information."""
    id: str
    name: str
    tenant_type: str
    status: str
    parent_id: Optional[str] = None
    created_at: datetime
    settings: strawberry.scalars.JSON
    limits: strawberry.scalars.JSON

@strawberry.type
class User:
    """User information."""
    id: str
    username: str
    email: str
    role: str
    permissions: List[str]
    created_at: datetime
    last_login: Optional[datetime] = None
    is_active: bool = True

@strawberry.type
class SyncStatus:
    """Overall sync status."""
    total_repositories: int
    synced_repositories: int
    total_issues: int
    synced_issues: int
    total_prs: int
    synced_prs: int
    last_sync: Optional[datetime] = None
    queue_size: int = 0
    active_tasks: int = 0

@strawberry.type
class ApiMetrics:
    """API metrics information."""
    total_requests: int
    successful_requests: int
    failed_requests: int
    average_response_time: float
    requests_per_minute: int

# Input Types
@strawberry.input
class RepositoryFilter:
    """Filter for repositories."""
    name: Optional[str] = None
    organization: Optional[str] = None
    sync_enabled: Optional[bool] = None

@strawberry.input
class IssueFilter:
    """Filter for issues."""
    repository: Optional[str] = None
    state: Optional[str] = None
    author: Optional[str] = None
    assignee: Optional[str] = None
    label: Optional[str] = None
    sync_status: Optional[str] = None

@strawberry.input
class SyncInput:
    """Input for sync operations."""
    repositories: List[str]
    include_prs: bool = False
    dry_run: bool = False

@strawberry.input
class TenantInput:
    """Input for creating tenants."""
    name: str
    tenant_type: str
    parent_id: Optional[str] = None
    settings: Optional[strawberry.scalars.JSON] = None
    limits: Optional[strawberry.scalars.JSON] = None

# GraphQL Context
@dataclass
class GraphQLContext:
    """GraphQL request context."""
    user_id: Optional[str] = None
    tenant_id: Optional[str] = None
    permissions: List[str] = None
    sync_engine: Optional[AsyncGiteaKimaiSync] = None
    security_manager: Optional[SecurityManager] = None
    metrics: Optional[MetricsCollector] = None

# Resolvers
@strawberry.type
class Query:
    """GraphQL Query root."""

    @strawberry.field
    async def repositories(self, info: Info, filter: Optional[RepositoryFilter] = None) -> List[Repository]:
        """Get repositories with optional filtering."""
        context: GraphQLContext = info.context

        # Check permissions
        if not context.permissions or "read" not in context.permissions:
            raise PermissionError("Insufficient permissions")

        # Mock data - replace with actual database queries
        repos = [
            Repository(
                name="example-repo",
                organization="example-org",
                description="Example repository",
                url="https://gitea.example.com/example-org/example-repo",
                last_sync=datetime.now(),
                issue_count=42,
                pr_count=15,
                sync_enabled=True
            )
        ]

        # Apply filters if provided
        if filter:
            if filter.name:
                repos = [r for r in repos if filter.name.lower() in r.name.lower()]
            if filter.organization:
                repos = [r for r in repos if r.organization == filter.organization]
            if filter.sync_enabled is not None:
                repos = [r for r in repos if r.sync_enabled == filter.sync_enabled]

        return repos

    @strawberry.field
    async def issues(self, info: Info, filter: Optional[IssueFilter] = None,
                    limit: int = 100, offset: int = 0) -> List[Issue]:
        """Get issues with optional filtering and pagination."""
        context: GraphQLContext = info.context

        if not context.permissions or "read" not in context.permissions:
            raise PermissionError("Insufficient permissions")

        # Mock data - replace with actual database queries
        issues = [
            Issue(
                id=1,
                number=1,
                title="Example issue",
                body="This is an example issue",
                state="open",
                author="user1",
                assignees=["user2"],
                labels=["bug", "priority-high"],
                created_at=datetime.now(),
                repository="example-repo",
                sync_status="synced"
            )
        ]

        # Apply filters
        if filter:
            if filter.repository:
                issues = [i for i in issues if i.repository == filter.repository]
            if filter.state:
                issues = [i for i in issues if i.state == filter.state]
            if filter.author:
                issues = [i for i in issues if i.author == filter.author]
            if filter.sync_status:
                issues = [i for i in issues if i.sync_status == filter.sync_status]

        # Apply pagination
        return issues[offset:offset + limit]

    @strawberry.field
    async def pull_requests(self, info: Info, repository: Optional[str] = None,
                           limit: int = 100, offset: int = 0) -> List[PullRequest]:
        """Get pull requests with optional filtering and pagination."""
        context: GraphQLContext = info.context

        if not context.permissions or "read" not in context.permissions:
            raise PermissionError("Insufficient permissions")

        # Mock data
        prs = [
            PullRequest(
                id=1,
                number=1,
                title="Example PR",
                body="This is an example pull request",
                state="open",
                author="user1",
                base_branch="main",
                head_branch="feature/example",
                created_at=datetime.now(),
                repository="example-repo",
                sync_status="synced"
            )
        ]

        if repository:
            prs = [pr for pr in prs if pr.repository == repository]

        return prs[offset:offset + limit]

    @strawberry.field
    async def kimai_projects(self, info: Info) -> List[KimaiProject]:
        """Get Kimai projects."""
        context: GraphQLContext = info.context

        if not context.permissions or "read" not in context.permissions:
            raise PermissionError("Insufficient permissions")

        # Mock data
        return [
            KimaiProject(
                id=1,
                name="Example Project",
                comment="Auto-created from Gitea",
                visible=True,
                created_at=datetime.now(),
                activities=[]
            )
        ]

    @strawberry.field
    async def sync_status(self, info: Info) -> SyncStatus:
        """Get overall synchronization status."""
        context: GraphQLContext = info.context

        if not context.permissions or "read" not in context.permissions:
            raise PermissionError("Insufficient permissions")

        if context.sync_engine:
            status = await context.sync_engine.get_sync_status()
            return SyncStatus(
                total_repositories=10,
                synced_repositories=8,
                total_issues=150,
                synced_issues=142,
                total_prs=45,
                synced_prs=43,
                last_sync=datetime.now(),
                queue_size=status.get('queue_status', {}).get('queue_size', 0),
                active_tasks=status.get('queue_status', {}).get('active_tasks', 0)
            )

        return SyncStatus(
            total_repositories=0,
            synced_repositories=0,
            total_issues=0,
            synced_issues=0,
            total_prs=0,
            synced_prs=0
        )

    @strawberry.field
    async def tenants(self, info: Info) -> List[Tenant]:
        """Get tenants (admin only)."""
        context: GraphQLContext = info.context

        if not context.permissions or "admin" not in context.permissions:
            raise PermissionError("Admin access required")

        tenants = tenant_manager.list_tenants()
        return [
            Tenant(
                id=t.id,
                name=t.name,
                tenant_type=t.tenant_type.value,
                status=t.status.value,
                parent_id=t.parent_id,
                created_at=t.created_at,
                settings=t.settings,
                limits=t.limits
            )
            for t in tenants
        ]

    @strawberry.field
    async def api_metrics(self, info: Info) -> ApiMetrics:
        """Get API metrics."""
        context: GraphQLContext = info.context

        if not context.permissions or "read" not in context.permissions:
            raise PermissionError("Insufficient permissions")

        if context.metrics:
            metrics = context.metrics.get_summary()
            return ApiMetrics(
                total_requests=metrics.get('total_requests', 0),
                successful_requests=metrics.get('successful_requests', 0),
                failed_requests=metrics.get('failed_requests', 0),
                average_response_time=metrics.get('average_response_time', 0.0),
                requests_per_minute=metrics.get('requests_per_minute', 0)
            )

        return ApiMetrics(
            total_requests=0,
            successful_requests=0,
            failed_requests=0,
            average_response_time=0.0,
            requests_per_minute=0
        )

@strawberry.type
class Mutation:
    """GraphQL Mutation root."""

    @strawberry.mutation
    async def trigger_sync(self, info: Info, input: SyncInput) -> List[SyncResult]:
        """Trigger synchronization for specified repositories."""
        context: GraphQLContext = info.context

        if not context.permissions or "write" not in context.permissions:
            raise PermissionError("Insufficient permissions")

        if not context.sync_engine:
            raise RuntimeError("Sync engine not available")

        # Trigger async sync
        result = await context.sync_engine.sync_repositories_async(
            input.repositories,
            include_prs=input.include_prs
        )

        # Convert results to GraphQL types
        sync_results = []
        for task_id, task_result in result.get('results', {}).items():
            sync_results.append(
                SyncResult(
                    id=task_id,
                    repository=task_result.get('repository', ''),
                    task_type="sync",
                    success=task_result.get('success', False),
                    items_processed=task_result.get('items_processed', 0),
                    items_created=task_result.get('items_created', 0),
                    items_updated=task_result.get('items_updated', 0),
                    duration=task_result.get('duration', 0.0),
                    error_message=task_result.get('error_message'),
                    timestamp=datetime.now()
                )
            )

        return sync_results

    @strawberry.mutation
    async def create_tenant(self, info: Info, input: TenantInput) -> Tenant:
        """Create a new tenant (admin only)."""
        context: GraphQLContext = info.context

        if not context.permissions or "admin" not in context.permissions:
            raise PermissionError("Admin access required")

        tenant_id = tenant_manager.create_tenant(
            name=input.name,
            tenant_type=TenantType(input.tenant_type),
            parent_id=input.parent_id,
            settings=input.settings,
            limits=input.limits
        )

        tenant = tenant_manager.get_tenant(tenant_id)
        if tenant:
            return Tenant(
                id=tenant.id,
                name=tenant.name,
                tenant_type=tenant.tenant_type.value,
                status=tenant.status.value,
                parent_id=tenant.parent_id,
                created_at=tenant.created_at,
                settings=tenant.settings,
                limits=tenant.limits
            )

        raise RuntimeError("Failed to create tenant")

    @strawberry.mutation
    async def update_repository_sync(self, info: Info, repository: str, enabled: bool) -> Repository:
        """Enable or disable sync for a repository."""
        context: GraphQLContext = info.context

        if not context.permissions or "write" not in context.permissions:
            raise PermissionError("Insufficient permissions")

        # Update repository sync status in database
        # This is a mock implementation
        return Repository(
            name=repository,
            organization="example-org",
            description="Example repository",
            url=f"https://gitea.example.com/example-org/{repository}",
            sync_enabled=enabled
        )

@strawberry.type
class Subscription:
    """GraphQL Subscription root for real-time updates."""

    @strawberry.subscription
    async def sync_progress(self, info: Info, repository: Optional[str] = None) -> AsyncIterator[SyncResult]:
        """Subscribe to sync progress updates."""
        context: GraphQLContext = info.context

        if not context.permissions or "read" not in context.permissions:
            raise PermissionError("Insufficient permissions")

        # Mock subscription - replace with actual event stream
        while True:
            await asyncio.sleep(5)  # Wait 5 seconds

            yield SyncResult(
                id=f"sync-{datetime.now().timestamp()}",
                repository=repository or "example-repo",
                task_type="sync",
                success=True,
                items_processed=10,
                items_created=2,
                items_updated=3,
                duration=1.5,
                timestamp=datetime.now()
            )

    @strawberry.subscription
    async def issue_updates(self, info: Info, repository: str) -> AsyncIterator[Issue]:
        """Subscribe to issue updates for a repository."""
        context: GraphQLContext = info.context

        if not context.permissions or "read" not in context.permissions:
            raise PermissionError("Insufficient permissions")

        # Mock subscription
        while True:
            await asyncio.sleep(10)  # Wait 10 seconds

            yield Issue(
                id=1,
                number=1,
                title="Updated issue",
                body="This issue was updated",
                state="open",
                author="user1",
                assignees=["user2"],
                labels=["bug"],
                created_at=datetime.now(),
                updated_at=datetime.now(),
                repository=repository,
                sync_status="synced"
            )

# Schema
schema = strawberry.Schema(
    query=Query,
    mutation=Mutation,
    subscription=Subscription
)

class GraphQLAPI:
    """GraphQL API server setup and management."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.sync_engine = None
        self.security_manager = SecurityManager()
        self.metrics = MetricsCollector()

    async def initialize(self):
        """Initialize GraphQL API dependencies."""
        self.sync_engine = AsyncGiteaKimaiSync()
        await self.sync_engine.initialize()
        logger.info("GraphQL API initialized")

    async def cleanup(self):
        """Cleanup GraphQL API dependencies."""
        if self.sync_engine:
            await self.sync_engine.cleanup()
        logger.info("GraphQL API cleaned up")

    def create_context(self, request) -> GraphQLContext:
        """Create GraphQL context from request."""
        # Extract user information from request headers/auth
        user_id = request.headers.get("X-User-ID")
        tenant_id = request.headers.get("X-Tenant-ID")
        auth_token = request.headers.get("Authorization", "").replace("Bearer ", "")

        permissions = []
        if auth_token:
            # Validate token and extract permissions
            # This is a simplified implementation
            permissions = ["read", "write"]

        return GraphQLContext(
            user_id=user_id,
            tenant_id=tenant_id,
            permissions=permissions,
            sync_engine=self.sync_engine,
            security_manager=self.security_manager,
            metrics=self.metrics
        )

    def get_router(self) -> GraphQLRouter:
        """Get FastAPI GraphQL router."""
        return GraphQLRouter(
            schema,
            context_getter=self.create_context,
            subscription_protocols=[
                GRAPHQL_TRANSPORT_WS_PROTOCOL,
                GRAPHQL_WS_PROTOCOL,
            ],
        )

# Global GraphQL API instance
graphql_api = None

async def create_graphql_api(config: Dict[str, Any]) -> GraphQLAPI:
    """Create and initialize GraphQL API."""
    global graphql_api
    graphql_api = GraphQLAPI(config)
    await graphql_api.initialize()
    return graphql_api

async def shutdown_graphql_api():
    """Shutdown GraphQL API."""
    global graphql_api
    if graphql_api:
        await graphql_api.cleanup()
        graphql_api = None
