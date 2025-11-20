"""
SQLite database schema for Edda framework.

This module defines the table schemas for storing workflow instances,
execution history, compensations, event subscriptions, and outbox events.
"""

# SQL schema for workflow definitions (source code storage)
WORKFLOW_DEFINITIONS_TABLE = """
CREATE TABLE IF NOT EXISTS workflow_definitions (
    workflow_name TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    source_code TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (workflow_name, source_hash)
);
"""

# Indexes for workflow definitions
WORKFLOW_DEFINITIONS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_definitions_name ON workflow_definitions(workflow_name);",
    "CREATE INDEX IF NOT EXISTS idx_definitions_hash ON workflow_definitions(source_hash);",
]

# SQL schema for workflow instances table with distributed locking support
WORKFLOW_INSTANCES_TABLE = """
CREATE TABLE IF NOT EXISTS workflow_instances (
    instance_id TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    owner_service TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    current_activity_id TEXT,
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    input_data TEXT NOT NULL,
    output_data TEXT,
    locked_by TEXT,
    locked_at TEXT,
    lock_timeout_seconds INTEGER,
    CONSTRAINT valid_status CHECK (
        status IN ('running', 'completed', 'failed', 'waiting_for_event', 'waiting_for_timer', 'compensating', 'cancelled')
    ),
    FOREIGN KEY (workflow_name, source_hash) REFERENCES workflow_definitions(workflow_name, source_hash)
);
"""

# Indexes for workflow instances
WORKFLOW_INSTANCES_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_instances_status ON workflow_instances(status);",
    "CREATE INDEX IF NOT EXISTS idx_instances_workflow ON workflow_instances(workflow_name);",
    "CREATE INDEX IF NOT EXISTS idx_instances_owner ON workflow_instances(owner_service);",
    "CREATE INDEX IF NOT EXISTS idx_instances_locked ON workflow_instances(locked_by, locked_at);",
    "CREATE INDEX IF NOT EXISTS idx_instances_updated ON workflow_instances(updated_at);",
    "CREATE INDEX IF NOT EXISTS idx_instances_hash ON workflow_instances(source_hash);",
]

# SQL schema for workflow execution history (for deterministic replay)
WORKFLOW_HISTORY_TABLE = """
CREATE TABLE IF NOT EXISTS workflow_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    activity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_data TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(instance_id) ON DELETE CASCADE,
    CONSTRAINT unique_instance_activity UNIQUE (instance_id, activity_id)
);
"""

# Indexes for workflow history
WORKFLOW_HISTORY_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_history_instance ON workflow_history(instance_id, activity_id);",
    "CREATE INDEX IF NOT EXISTS idx_history_created ON workflow_history(created_at);",
]

# SQL schema for compensation transactions (LIFO stack for Saga pattern)
WORKFLOW_COMPENSATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS workflow_compensations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    activity_id TEXT NOT NULL,
    activity_name TEXT NOT NULL,
    args TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(instance_id) ON DELETE CASCADE
);
"""

# Indexes for workflow compensations
WORKFLOW_COMPENSATIONS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_compensations_instance ON workflow_compensations(instance_id, created_at DESC);",
]

# SQL schema for event subscriptions (for wait_event)
WORKFLOW_EVENT_SUBSCRIPTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS workflow_event_subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    activity_id TEXT,
    timeout_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(instance_id) ON DELETE CASCADE,
    CONSTRAINT unique_instance_event UNIQUE (instance_id, event_type)
);
"""

# Indexes for event subscriptions
WORKFLOW_EVENT_SUBSCRIPTIONS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_subscriptions_event ON workflow_event_subscriptions(event_type);",
    "CREATE INDEX IF NOT EXISTS idx_subscriptions_timeout ON workflow_event_subscriptions(timeout_at);",
    "CREATE INDEX IF NOT EXISTS idx_subscriptions_instance ON workflow_event_subscriptions(instance_id);",
]

# SQL schema for timer subscriptions (for wait_timer)
WORKFLOW_TIMER_SUBSCRIPTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS workflow_timer_subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    timer_id TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    activity_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(instance_id) ON DELETE CASCADE,
    CONSTRAINT unique_instance_timer UNIQUE (instance_id, timer_id)
);
"""

# Indexes for timer subscriptions
WORKFLOW_TIMER_SUBSCRIPTIONS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_timer_subscriptions_expires ON workflow_timer_subscriptions(expires_at);",
    "CREATE INDEX IF NOT EXISTS idx_timer_subscriptions_instance ON workflow_timer_subscriptions(instance_id);",
]

# SQL schema for transactional outbox pattern
OUTBOX_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS outbox_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    event_source TEXT NOT NULL,
    event_data TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/json',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    published_at TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    retry_count INTEGER DEFAULT 0,
    last_error TEXT,
    CONSTRAINT valid_outbox_status CHECK (status IN ('pending', 'published', 'failed'))
);
"""

# SQL schema for schema version tracking
SCHEMA_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (datetime('now')),
    description TEXT NOT NULL
);
"""

# Indexes for outbox events
OUTBOX_EVENTS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_events(status, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_outbox_retry ON outbox_events(status, retry_count);",
    "CREATE INDEX IF NOT EXISTS idx_outbox_published ON outbox_events(published_at);",
]

# Current schema version
CURRENT_SCHEMA_VERSION = 1

# All table creation statements
ALL_TABLES = [
    SCHEMA_VERSION_TABLE,
    WORKFLOW_DEFINITIONS_TABLE,
    WORKFLOW_INSTANCES_TABLE,
    WORKFLOW_HISTORY_TABLE,
    WORKFLOW_COMPENSATIONS_TABLE,
    WORKFLOW_EVENT_SUBSCRIPTIONS_TABLE,
    WORKFLOW_TIMER_SUBSCRIPTIONS_TABLE,
    OUTBOX_EVENTS_TABLE,
]

# All index creation statements
ALL_INDEXES = (
    WORKFLOW_DEFINITIONS_INDEXES
    + WORKFLOW_INSTANCES_INDEXES
    + WORKFLOW_HISTORY_INDEXES
    + WORKFLOW_COMPENSATIONS_INDEXES
    + WORKFLOW_EVENT_SUBSCRIPTIONS_INDEXES
    + WORKFLOW_TIMER_SUBSCRIPTIONS_INDEXES
    + OUTBOX_EVENTS_INDEXES
)
