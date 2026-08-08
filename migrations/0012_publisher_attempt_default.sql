-- Owner module: publisher
-- Keep upgrades of databases that already applied the foundation migration
-- compatible with the repository's running-attempt insert path.
ALTER TABLE publication_attempts
    ALTER COLUMN status SET DEFAULT 'running';
