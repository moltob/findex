CREATE TABLE file1 (
    path TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    hash TEXT NOT NULL,
    created TIMESTAMP NULL,
    modified TIMESTAMP NULL
);

CREATE TABLE file2 (
    path TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    hash TEXT NOT NULL,
    created TIMESTAMP NULL,
    modified TIMESTAMP NULL
);

CREATE INDEX idx_hash1 ON file1 (hash);
CREATE INDEX idx_hash2 ON file2 (hash);
