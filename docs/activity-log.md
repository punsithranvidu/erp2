# ERP activity log

The admin-only `/activity-log` page records business data changes from deployment onward. There is no retroactive reconstruction, Undo, manual deletion or export of log data in this version.

## Coverage

`routes/activity_log.py:TABLE_MODULES` is the explicit registry of 35 business tables covering all 14 module areas: finance, petty cash, invoices, documents, marketing emails, HS codes, weekly tasks, worksheet, calendar, attendance, messages, notes, achievements, and employee access/permissions. Child records, memberships, allocations, permissions, bulk imports and status changes are included. Login, failed credential checks and explicit logout are separate authentication events. Repeated blocked login requests are not appended again. A failed sign-in records an attempted username, not an authenticated identity.

Startup attaches missing PostgreSQL row triggers to existing tables. Each module's first-use schema initializer then attaches any newly created tables before its route handler runs. New application tables must be added to `TABLE_MODULES`; there is a coverage test. Notification polling state, duplicate module history tables, login limiter/session storage, schema migration bookkeeping and OAuth token storage are excluded. Direct Google Drive changes and external file operations that do not change a corresponding ERP database record are outside this log. Browser closure cannot be reliably detected as logout; session heartbeats are not logged.

Changes are captured inside their original database transaction: an audit insertion failure aborts the corresponding write. A no-op SQL update produces no entry. Each row in a bulk operation produces its own entry, sharing a server-generated request reference. Rows with composite identities show their key fields. Soft deletion stores its pre-delete record snapshot as well as field changes; physical deletion stores the removed row. Restoring a soft-deleted row is a new event.

The shared psycopg cursor supplies session identity, request reference, route, reported IP and browser. Context is transaction-local and reapplied after commits/rollbacks, including reused cursors. Neither request JSON nor a username supplied in a form can impersonate the actor of a business write. A reported IP/browser can be spoofed and is supporting context, not physical-device identification. Writes outside Flask are labelled System.

## Privacy and permissions

Creation and deletion store sanitized text/JSON snapshots, and edits store only changed fields with before/after values. Password, token, secret, session and credential keys are recursively redacted, including changed password hashes. Files/binaries and HTTP request bodies are not copied. Business descriptions, messages and employee fields can appear in snapshots: only active administrators may read them. Every read checks the current user record, so an old signed admin session cannot read the log after demotion or account disablement. Responses are `no-store` and the UI uses text nodes for all record content.

The audit table has row-level security enabled and no anonymous/authenticated Supabase or public grants. PostgreSQL triggers reject all log updates, truncation and deletion of records younger than 90 days. The application exposes only GET routes. **A database owner/superuser can disable triggers or change schema; this is not an external tamper-proof archive.** The current app connection also performs schema migrations and cannot have its owner privileges removed without a separate deployment-role migration. Do not claim protection against someone with database-owner credentials. For that threat model, separate migration/runtime database roles and replicate events to independent storage.

## Retention and storage

The agreed retention is 90 days, with no permanent archive. Old entries disappear from queries at the boundary and are removed in bounded 2,000-row batches at most hourly per active worker. Cleanup runs during authenticated app use; an inactive app does not need to remain awake for housekeeping. Cleanup failure is logged and deferred rather than blocking normal app use. The time index supports expiry selection; keyset pagination fetches 50 entries at a time. Filters include user, module, action, exact record key and Sri Lanka date boundaries. Existing rows retain original database values; timestamps are displayed in Asia/Colombo.

The admin screen reports `pg_total_relation_size` for the log, including indexes/TOAST. Storage depends on real activity and text lengths. The implementation stores no duplicate uploads, does not log page clicks/polling, and does not query history on ordinary page loads. Watch measured usage before deciding whether 90 days remains suitable for the free database plan. Cleanup reuses PostgreSQL space; it does not necessarily shrink allocated database files immediately.

## Validation

Normal unit suite: `PYTHONPATH=<temporary-psycopg-folder> python -m unittest discover -s tests -q`.

`tests/test_activity_postgres.py` additionally uses a disposable PostgreSQL database named **erp_audit_test**, bound to **127.0.0.1**, via `ERP_AUDIT_TEST_DSN`. It refuses other database names/hosts. These tests cover every registered business table, exact field diffs, snapshots, secret redaction, atomic rollback, identity after commit, immutability/retention and live admin revocation. Do not point them at production.
