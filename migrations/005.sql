ALTER TABLE outbox ADD COLUMN finished_at INTEGER;

UPDATE outbox SET finished_at = unixepoch()
WHERE kind = 'handoff' AND status IN ('not_sent', 'cancelled');

DROP INDEX outbox_one_handoff;

CREATE UNIQUE INDEX outbox_one_handoff ON outbox (portal, chat, session)
WHERE kind = 'handoff' AND status IN ('planned', 'in_flight', 'api_accepted', 'confirmed', 'uncertain');

CREATE TABLE crm_desired (
    id INTEGER PRIMARY KEY,
    portal TEXT NOT NULL,
    chat TEXT NOT NULL,
    session TEXT NOT NULL,
    revision INTEGER NOT NULL,
    version INTEGER NOT NULL,
    line TEXT NOT NULL,
    connector TEXT NOT NULL,
    fields TEXT NOT NULL,
    create_type INTEGER,
    action_id INTEGER,
    reason TEXT,
    UNIQUE (portal, chat, session)
);

INSERT INTO crm_desired (
    portal, chat, session, revision, version, line, connector, fields, create_type, action_id
)
SELECT o.portal, o.chat, o.session, o.revision, o.version, o.line, o.connector,
       json_extract(o.params, '$.fields'),
       CASE WHEN o.kind = 'crm_create' THEN json_extract(o.params, '$.entityTypeId') END,
       o.id
FROM outbox AS o
WHERE o.kind IN ('crm_create', 'crm_update') AND o.id = (
    SELECT max(previous.id) FROM outbox AS previous
    WHERE (previous.portal, previous.chat, previous.session) = (o.portal, o.chat, o.session)
      AND previous.kind IN ('crm_create', 'crm_update')
);

CREATE TABLE handoff_waiting (
    portal TEXT NOT NULL,
    chat TEXT NOT NULL,
    session TEXT NOT NULL,
    revision INTEGER NOT NULL,
    version INTEGER NOT NULL,
    line TEXT NOT NULL,
    connector TEXT NOT NULL,
    blocked_by INTEGER NOT NULL,
    PRIMARY KEY (portal, chat, session)
);
