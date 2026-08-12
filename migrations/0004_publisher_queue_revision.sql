ALTER TABLE posts
    ADD COLUMN revision bigint NOT NULL DEFAULT 0;

ALTER TABLE posts
    ADD CONSTRAINT posts_revision_nonnegative CHECK (revision >= 0);
