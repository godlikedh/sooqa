-- Owner module: library
-- E1 used a unique SHA-256 index for the initial table skeleton. Exact
-- duplicate resolution needs to retain hashes for original and derived assets,
-- while enforcing uniqueness for canonical assets specifically.
DROP INDEX media_assets_sha256_idx;

CREATE INDEX media_assets_sha256_idx
    ON media_assets (sha256)
    WHERE sha256 IS NOT NULL;

CREATE UNIQUE INDEX media_assets_canonical_sha256_idx
    ON media_assets (sha256)
    WHERE sha256 IS NOT NULL AND role = 'canonical';

CREATE UNIQUE INDEX media_assets_content_canonical_idx
    ON media_assets (content_item_id)
    WHERE role = 'canonical';
