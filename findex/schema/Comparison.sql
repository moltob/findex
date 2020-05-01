CREATE TABLE file (
    hash TEXT NOT NULL,
    origin INTEGER NOT NULL,  -- ID of index where file comes from
    path TEXT NOT NULL,
    size INTEGER NOT NULL,
    created TIMESTAMP NULL,
    modified TIMESTAMP NULL
);

CREATE INDEX idx_hash ON file (hash);
CREATE INDEX idx_path ON file (path);
CREATE UNIQUE INDEX idx_path_from ON file (origin,path);
