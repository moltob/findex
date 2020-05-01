CREATE TABLE file (
    path TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    hash TEXT NOT NULL,
    created TIMESTAMP NULL,
    modified TIMESTAMP NULL
);

CREATE INDEX idx_hash ON file (hash);
