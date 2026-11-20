CREATE TABLE inbox (
    sequence INTEGER PRIMARY KEY,
    portal TEXT NOT NULL,
    chat TEXT NOT NULL,
    session TEXT NOT NULL,
    message TEXT NOT NULL,
    kind TEXT NOT NULL,
    processed INTEGER NOT NULL DEFAULT 0 CHECK (processed IN (0, 1)),
    UNIQUE (portal, chat, session, message)
);

CREATE TABLE sessions (
    portal TEXT NOT NULL,
    chat TEXT NOT NULL,
    session TEXT NOT NULL,
    state TEXT NOT NULL,
    PRIMARY KEY (portal, chat, session)
);
