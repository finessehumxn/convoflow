# 3. Mask card numbers and SSNs before storage

Status: accepted

## Context
A compliance scanner that stores the card numbers it flags becomes the biggest compliance risk in the system. Encrypting the disk does not help when a query, backup or log can read the column.

## Decision
`pii.mask` runs at enqueue and again in the worker, before any write. A valid card (13 to 19 digits that pass Luhn) becomes `[card ****1111 fp:xxxxxxxx]`, and a valid SSN (not a number the SSA never issues) becomes `[ssn ***-**-6789 fp:xxxxxxxx]`. The fingerprint is an HMAC with `CONVOFLOW_PII_KEY`, so the same value can be linked across messages without being stored. Dead letters store the masked payload. The scanner's card and SSN rules match these tokens.

## Consequences
- No table holds a raw card number or SSN. `test_pii.py` checks the inbox, messages, dead letters and findings.
- Luhn and SSA validity cut false positives from order and phone numbers.
- The raw value cannot be recovered, by design. A customer who needs it must keep it in their own system.
- Rotating `CONVOFLOW_PII_KEY` changes fingerprints, so links across the rotation are lost.
- Detection inside free text is regex based. Numbers spelled out in words, or split across messages, are missed.
