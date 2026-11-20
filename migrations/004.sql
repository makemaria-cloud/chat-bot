ALTER TABLE sessions ADD COLUMN version INTEGER NOT NULL DEFAULT 0;

CREATE TABLE crm_links (
    portal TEXT NOT NULL,
    chat TEXT NOT NULL,
    session TEXT NOT NULL,
    entity_type INTEGER NOT NULL CHECK (entity_type > 0),
    item INTEGER NOT NULL CHECK (item > 0),
    PRIMARY KEY (portal, chat, session)
);

CREATE TABLE outbox (
    id INTEGER PRIMARY KEY,
    portal TEXT NOT NULL,
    chat TEXT NOT NULL,
    session TEXT NOT NULL,
    revision INTEGER NOT NULL,
    version INTEGER NOT NULL,
    line TEXT NOT NULL,
    connector TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('message', 'handoff', 'crm_create', 'crm_update')),
    dedup TEXT NOT NULL,
    params TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'planned' CHECK (status IN (
        'planned', 'in_flight', 'api_accepted', 'confirmed', 'not_sent', 'uncertain', 'cancelled'
    )),
    reason TEXT,
    claimed_at INTEGER,
    UNIQUE (portal, chat, session, kind, dedup)
);

CREATE INDEX outbox_planned ON outbox (id) WHERE status = 'planned';
CREATE INDEX outbox_in_flight ON outbox (portal, chat) WHERE status = 'in_flight';
CREATE UNIQUE INDEX outbox_one_crm_create ON outbox (portal, chat)
WHERE kind = 'crm_create' AND status != 'cancelled';
CREATE UNIQUE INDEX outbox_one_handoff ON outbox (portal, chat, session)
WHERE kind = 'handoff' AND status != 'cancelled';
