-- Which messages the model-based rules have looked at. Messages stored before this
-- migration are marked checked, so switching the rules on does not bill a backlog.
ALTER TABLE messages ADD COLUMN llm_checked_at timestamptz;
UPDATE messages SET llm_checked_at = ingested_at;
CREATE INDEX messages_llm_unchecked ON messages (ingested_at) WHERE llm_checked_at IS NULL;
