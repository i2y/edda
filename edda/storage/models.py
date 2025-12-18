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
    framework TEXT NOT NULL DEFAULT 'python',
    status TEXT NOT NULL DEFAULT 'running',
    current_activity_id TEXT,
    continued_from TEXT,
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    input_data TEXT NOT NULL,
    output_data TEXT,
    locked_by TEXT,
    locked_at TEXT,
    lock_timeout_seconds INTEGER,
    CONSTRAINT valid_status CHECK (
        status IN ('running', 'completed', 'failed', 'waiting_for_event', 'waiting_for_timer', 'waiting_for_message', 'compensating', 'cancelled', 'recurred')
    ),
    FOREIGN KEY (continued_from) REFERENCES workflow_instances(instance_id),
    FOREIGN KEY (workflow_name, source_hash) REFERENCES workflow_definitions(workflow_name, source_hash)
);
"""

# Indexes for workflow instances
WORKFLOW_INSTANCES_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_instances_status ON workflow_instances(status);",
    "CREATE INDEX IF NOT EXISTS idx_instances_workflow ON workflow_instances(workflow_name);",
    "CREATE INDEX IF NOT EXISTS idx_instances_owner ON workflow_instances(owner_service);",
    "CREATE INDEX IF NOT EXISTS idx_instances_framework ON workflow_instances(framework);",
    "CREATE INDEX IF NOT EXISTS idx_instances_locked ON workflow_instances(locked_by, locked_at);",
    "CREATE INDEX IF NOT EXISTS idx_instances_updated ON workflow_instances(updated_at);",
    "CREATE INDEX IF NOT EXISTS idx_instances_hash ON workflow_instances(source_hash);",
    "CREATE INDEX IF NOT EXISTS idx_instances_continued_from ON workflow_instances(continued_from);",
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

# SQL schema for archived workflow history (for recur pattern)
WORKFLOW_HISTORY_ARCHIVE_TABLE = """
CREATE TABLE IF NOT EXISTS workflow_history_archive (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    activity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_data TEXT NOT NULL,
    created_at TEXT NOT NULL,
    archived_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(instance_id) ON DELETE CASCADE
);
"""

# Indexes for workflow history archive
WORKFLOW_HISTORY_ARCHIVE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_history_archive_instance ON workflow_history_archive(instance_id);",
    "CREATE INDEX IF NOT EXISTS idx_history_archive_archived ON workflow_history_archive(archived_at);",
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

# SQL schema for group memberships (Erlang pg style)
WORKFLOW_GROUP_MEMBERSHIPS_TABLE = """
CREATE TABLE IF NOT EXISTS workflow_group_memberships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    group_name TEXT NOT NULL,
    joined_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(instance_id) ON DELETE CASCADE,
    CONSTRAINT unique_instance_group UNIQUE (instance_id, group_name)
);
"""

# Indexes for group memberships
WORKFLOW_GROUP_MEMBERSHIPS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_group_memberships_group ON workflow_group_memberships(group_name);",
    "CREATE INDEX IF NOT EXISTS idx_group_memberships_instance ON workflow_group_memberships(instance_id);",
]

# =============================================================================
# Channel-based Message Queue System
# =============================================================================

# SQL schema for channel messages (persistent message queue)
CHANNEL_MESSAGES_TABLE = """
CREATE TABLE IF NOT EXISTS channel_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel TEXT NOT NULL,
    message_id TEXT NOT NULL UNIQUE,
    data_type TEXT NOT NULL,
    data TEXT,
    data_binary BLOB,
    metadata TEXT,
    published_at TEXT NOT NULL DEFAULT (datetime('now')),
    CONSTRAINT valid_data_type CHECK (data_type IN ('json', 'binary')),
    CONSTRAINT data_type_consistency CHECK (
        (data_type = 'json' AND data IS NOT NULL AND data_binary IS NULL) OR
        (data_type = 'binary' AND data IS NULL AND data_binary IS NOT NULL)
    )
);
"""

# Indexes for channel messages
CHANNEL_MESSAGES_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_channel_messages_channel ON channel_messages(channel, published_at);",
    "CREATE INDEX IF NOT EXISTS idx_channel_messages_id ON channel_messages(id);",
]

# SQL schema for channel subscriptions
CHANNEL_SUBSCRIPTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS channel_subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    mode TEXT NOT NULL,
    activity_id TEXT,
    cursor_message_id INTEGER,
    subscribed_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(instance_id) ON DELETE CASCADE,
    CONSTRAINT valid_mode CHECK (mode IN ('broadcast', 'competing')),
    CONSTRAINT unique_instance_channel UNIQUE (instance_id, channel)
);
"""

# Indexes for channel subscriptions
CHANNEL_SUBSCRIPTIONS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_channel_subscriptions_channel ON channel_subscriptions(channel);",
    "CREATE INDEX IF NOT EXISTS idx_channel_subscriptions_instance ON channel_subscriptions(instance_id);",
    "CREATE INDEX IF NOT EXISTS idx_channel_subscriptions_waiting ON channel_subscriptions(channel, activity_id);",
]

# SQL schema for channel delivery cursors (broadcast mode: track who read what)
CHANNEL_DELIVERY_CURSORS_TABLE = """
CREATE TABLE IF NOT EXISTS channel_delivery_cursors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    last_delivered_id INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(instance_id) ON DELETE CASCADE,
    CONSTRAINT unique_channel_instance UNIQUE (channel, instance_id)
);
"""

# Indexes for channel delivery cursors
CHANNEL_DELIVERY_CURSORS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_channel_delivery_cursors_channel ON channel_delivery_cursors(channel);",
]

# SQL schema for channel message claims (competing mode: who is processing what)
CHANNEL_MESSAGE_CLAIMS_TABLE = """
CREATE TABLE IF NOT EXISTS channel_message_claims (
    message_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    claimed_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (message_id) REFERENCES channel_messages(message_id) ON DELETE CASCADE,
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(instance_id) ON DELETE CASCADE
);
"""

# Indexes for channel message claims
CHANNEL_MESSAGE_CLAIMS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_channel_message_claims_instance ON channel_message_claims(instance_id);",
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
    CONSTRAINT valid_outbox_status CHECK (status IN ('pending', 'processing', 'published', 'failed', 'invalid', 'expired'))
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
    WORKFLOW_HISTORY_ARCHIVE_TABLE,
    WORKFLOW_COMPENSATIONS_TABLE,
    WORKFLOW_TIMER_SUBSCRIPTIONS_TABLE,
    WORKFLOW_GROUP_MEMBERSHIPS_TABLE,
    OUTBOX_EVENTS_TABLE,
    # Channel-based Message Queue System
    CHANNEL_MESSAGES_TABLE,
    CHANNEL_SUBSCRIPTIONS_TABLE,
    CHANNEL_DELIVERY_CURSORS_TABLE,
    CHANNEL_MESSAGE_CLAIMS_TABLE,
]

# All index creation statements
ALL_INDEXES = (
    WORKFLOW_DEFINITIONS_INDEXES
    + WORKFLOW_INSTANCES_INDEXES
    + WORKFLOW_HISTORY_INDEXES
    + WORKFLOW_HISTORY_ARCHIVE_INDEXES
    + WORKFLOW_COMPENSATIONS_INDEXES
    + WORKFLOW_TIMER_SUBSCRIPTIONS_INDEXES
    + WORKFLOW_GROUP_MEMBERSHIPS_INDEXES
    + OUTBOX_EVENTS_INDEXES
    # Channel-based Message Queue System
    + CHANNEL_MESSAGES_INDEXES
    + CHANNEL_SUBSCRIPTIONS_INDEXES
    + CHANNEL_DELIVERY_CURSORS_INDEXES
    + CHANNEL_MESSAGE_CLAIMS_INDEXES
)
