# dcre-sxr

SBSR response-leg reader (SCRUM-26, M4). Ingests synthetic pain.002-family SBSR reply files (token _SBSR) into sbsr_resp: one row per Tx block. Replay-safe via INSERT ... ON CONFLICT (response_file, e2e). 3-tier: ReaderTasklet -> ReaderService -> data/repo. [SYNTHETIC-CONTRACT R-35] reply shape.
