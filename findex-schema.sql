CREATE TABLE file (
    path TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    hash TEXT NOT NULL
);

CREATE INDEX idx_hash ON file (hash);
