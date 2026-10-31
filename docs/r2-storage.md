# Private R2 files and Drive migration

## Deploy without switching

Keep the existing Google configuration. Add `R2_BUCKET`, `R2_ENDPOINT_URL`, `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY` privately in Render. The S3 endpoint must be the root HTTPS Cloudflare R2 endpoint. The bucket stays private, Standard storage; credentials need Object Read & Write scoped only to this bucket. Never put them in source, screenshots, or chat.

The default `FILE_STORAGE_BACKEND=drive` deliberately leaves new uploads and displayed legacy links on Drive. Adding credentials alone does not activate R2. Deploy this code and open **Documents → Storage setup / R2 migration** as an active administrator. Check the connection: a tiny test object is uploaded, downloaded, SHA-256 verified and deleted. The app does not expose keys. Actual verification must run on Render because its private credentials are not present locally.

Take a Supabase database backup and retain the matching old code revision before migration. Originals and existing Drive link/ID columns are never replaced by this feature. New metadata tables `storage_files` and `storage_references` are private, RLS-enabled, and included in the ERP activity log.

## Copy and verify

Inventory looks at explicit ERP document/attachment IDs, finance proof/invoice/PO/quotation/folder URLs, petty cash proof links and employee file links. It does not scan arbitrary personal Drive content. Referenced folder pages are expanded as copy work proceeds, and child files inherit the original ERP reference for permission checking. If files were added directly to Drive but their ERP root folder is not referenced/indexed, use the existing Drive sync first; the inventory is not a guarantee that the entire Google account's quoted 2.59 GB belongs to the ERP.

Copy one file or folder page at a time. Upload/download uses spooled temporary files (1 MB RAM threshold) with a **25 MB per-file browser migration limit** to bound memory/network work on the free Render service. Large files stay on Drive and are listed for a separate offline transfer. Editable Google-native files are not silently exported to PDF; they remain on Drive until an export/migration choice is made. Shared-drive shortcuts and unsupported formats likewise need review.

Binary copies compare the downloaded Drive MD5 when available, upload to R2, read the R2 object back, verify SHA-256, and check Drive's modification timestamp again. Only then is the manifest marked verified. Failed attempts go to review. Folder traversal is paginated (100 children per page). Repeated inventory is idempotent by Drive ID; work uses database row locks and a five-minute lease to avoid simultaneous workers writing the same object. Following a killed request, wait five minutes and resume to reclaim an interrupted item.

Keep Storage setup open while copying. It spaces requests to avoid hammering the existing API limiter. Pause finishes the current file. Verified copies survive closing the page, a restart, or session expiry. Copying does not bypass the ERP idle timeout; sign in again and resume if needed. Timeouts, permissions and unsupported files appear in the review list. Counts with pending/copying/review items are not proof of completion. Retry review items only after addressing the cause. Files changed in Google Drive after their verified copy are not automatically synchronized: keep Drive originals stable at cutover.

## Switch and fallback

After all required files are verified (or explicitly accepted as Drive exceptions), test document access, attachments, a finance proof link, an unauthorized employee, and both Trash/Restore. Set **FILE_STORAGE_BACKEND=r2** in Render and redeploy. New Documents and Messages uploads then use R2. Existing verified links are supplied separately as `_storage_links` in API responses; source link fields stay unchanged, including when editing a finance record. Existing Drive folders used by finance can open as permission-checked ERP folder listings. New document folder structure is virtual, maintained by the existing ERP parent IDs.

Downloads require a current ERP login and current record/conversation permissions; knowing a file URL is insufficient. Original record references are checked each time, so unlinking/deleting a source record revokes its grant. Safe image/PDF/plain-text MIME types can display inline; HTML/SVG/other content downloads with nosniff/sandbox headers. No public bucket, public domain, or CORS configuration is needed. Drive originals retain their existing Google sharing: changing ERP permissions controls the R2 copy, not previously shared Google URLs. Review old Google shares before treating an ERP revocation as revocation of backup access. Existing migrated files can fall back to their original Google URL if R2 is unavailable. Files created only in R2 cannot fall back to Drive.

To revert the storage choice, set **FILE_STORAGE_BACKEND=drive** with this version still installed. It restores displayed original links and new Drive uploads without undoing unrelated user work. Keep R2 settings so R2-only uploads remain downloadable. Drive-mode uploads must use existing Drive-backed folders; R2-only folders do not have a Google counterpart. CSV exports retain original Google URLs in this staged version, so retain originals until export mappings are also migrated. **Deploying the old pre-R2 code alone cannot open files uploaded only to R2.** Those files must first be copied back and mapped to Drive, or the compatible integration code must remain installed. A database snapshot restore is not a casual rollback: it would discard newer business changes.

ERP soft-trash retains R2 objects for restore. Permanently removed R2-only uploads are cleaned after their last document/message reference is removed. Cleanup errors are logged/deferred. Migrated originals and their R2 copies are deliberately retained for fallback rather than automatically purged. Review orphaned storage separately before deleting backups. No original Google files are deleted in R2 mode.

## Test isolation

`tests/test_object_storage.py` includes unit tests and optional PostgreSQL/S3-emulator tests. Supply `ERP_R2_TEST_DSN` only for the disposable local **erp_audit_smoke** database on **127.0.0.1**; tests refuse other database names/hosts. Moto/boto3 test credentials are synthetic; there is no production bucket access. Moto is not a production dependency.
