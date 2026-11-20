ALTER TABLE inbox RENAME TO inbox_v1;

CREATE TABLE inbox (
    sequence INTEGER PRIMARY KEY,
    portal TEXT NOT NULL,
    chat TEXT NOT NULL,
    session TEXT NOT NULL,
    message TEXT NOT NULL,
    kind TEXT NOT NULL,
    event_id TEXT NOT NULL,
    processed INTEGER NOT NULL DEFAULT 0 CHECK (processed IN (0, 1)),
    UNIQUE (portal, chat, session, kind, event_id)
);

INSERT INTO inbox (sequence, portal, chat, session, message, kind, event_id, processed)
SELECT sequence, portal, chat, session, message, kind, message, processed FROM inbox_v1;

DROP TABLE inbox_v1;
