ALTER TABLE inbox ADD COLUMN context TEXT;

CREATE TABLE chat_heads (
    portal TEXT NOT NULL,
    chat TEXT NOT NULL,
    session TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    owner TEXT NOT NULL CHECK (owner IN ('bot', 'human', 'closed', 'stopped')),
    PRIMARY KEY (portal, chat)
);

CREATE INDEX inbox_pending ON inbox (sequence) WHERE processed = 0;
