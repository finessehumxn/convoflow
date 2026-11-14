-- Messages stored before the model rules are switched on (load tests included) are
-- marked checked, so turning the rules on only sends new traffic to the model.
UPDATE messages SET llm_checked_at = ingested_at WHERE llm_checked_at IS NULL;
