"""Private R2 storage, resumable Drive copying, and permission-checked file access."""
import hashlib
import io
import os
import re
import secrets
import tempfile
from urllib.parse import urlsplit, parse_qs

from flask import Blueprint, current_app, jsonify, redirect, request, session, url_for, render_template, send_file
from psycopg import sql
from .activity_log import admin_only

storage_bp = Blueprint('object_storage', __name__)
MAX_BYTES = 25 * 1024 * 1024
# Only ERP references are inventoried; no access to arbitrary personal Drive files.
REFERENCES = {
 'doc_items': ('DOCUMENT_STORAGE', ['drive_id']),
 'message_messages': ('MESSAGES', ['attachment_drive_id','attachment_url']),
 'finance_records': ('FINANCE', ['proof_link','invoice_link','po_link','quotation_link','folder_link']),
 'cash_advances': ('CASH_ADVANCES',['proof_link']),
 'cash_advance_expenses': ('CASH_ADVANCES',['proof_link']),
 'cash_advance_topups': ('CASH_ADVANCES',['proof_link']),
 'users': ('USERS',['file_link']),
}


@storage_bp.before_request
def protect_storage_writes():
    if request.method=='POST':
        origin=request.headers.get('Origin')
        if request.headers.get('X-ERP-Storage')!='1' or (origin and urlsplit(origin).netloc!=request.host):
            return jsonify(error='Invalid storage request. Reload Storage setup and try again.'),403


def db():
    from .db import connect
    return connect(current_app.config['DATABASE_URL'])


def enabled():
    return current_app.config.get('FILE_STORAGE_BACKEND', 'drive') == 'r2'


def drive_id(value, raw=False):
    if not value or not isinstance(value,str) or value.startswith('r2:'):return None
    if raw:return value if re.fullmatch(r'[\w-]{10,200}',value) else None
    parsed=urlsplit(value)
    if parsed.scheme not in ('http','https') or parsed.hostname not in ('drive.google.com','docs.google.com'):return None
    match=re.search(r'/(?:d|folders)/([\w-]+)',parsed.path)
    candidate=match.group(1) if match else (parse_qs(parsed.query).get('id') or [None])[0]
    return candidate if candidate and re.fullmatch(r'[\w-]{10,200}',candidate) else None


def client():
    import boto3
    from botocore.config import Config
    names=('R2_BUCKET','R2_ENDPOINT_URL','R2_ACCESS_KEY_ID','R2_SECRET_ACCESS_KEY')
    values={name:current_app.config.get(name) or os.environ.get(name,'') for name in names}
    if not all(values.values()):raise ValueError('The R2 connection settings are incomplete in Render.')
    endpoint=urlsplit(values['R2_ENDPOINT_URL'])
    if endpoint.scheme!='https' or not (endpoint.hostname or '').endswith('.r2.cloudflarestorage.com') or endpoint.path not in ('','/') or endpoint.query or endpoint.username:
        raise ValueError('Use the Cloudflare R2 S3 endpoint, with no bucket name or credentials in the URL.')
    return boto3.client('s3',endpoint_url=values['R2_ENDPOINT_URL'].rstrip('/'),region_name='auto',
        aws_access_key_id=values['R2_ACCESS_KEY_ID'],aws_secret_access_key=values['R2_SECRET_ACCESS_KEY'],
        config=Config(signature_version='s3v4',connect_timeout=4,read_timeout=8,retries={'total_max_attempts':1},
                      request_checksum_calculation='when_required',response_checksum_validation='when_required'))


def bucket():return current_app.config.get('R2_BUCKET') or os.environ['R2_BUCKET']


def init_storage(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS storage_files (
      id TEXT PRIMARY KEY, source_id TEXT UNIQUE, parent_id TEXT,
      name TEXT NOT NULL DEFAULT '', mime TEXT NOT NULL DEFAULT 'application/octet-stream',
      object_key TEXT, sha256 TEXT, size_bytes BIGINT NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'pending', error TEXT NOT NULL DEFAULT '',
      scan_page TEXT, copy_started TIMESTAMPTZ, created_by TEXT NOT NULL, verified_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now())''')
    conn.execute('''CREATE TABLE IF NOT EXISTS storage_references (
      file_id TEXT NOT NULL REFERENCES storage_files(id), table_name TEXT NOT NULL,
      row_id BIGINT NOT NULL, column_name TEXT NOT NULL, original_value TEXT NOT NULL,
      PRIMARY KEY(file_id,table_name,row_id,column_name))''')
    conn.execute('CREATE INDEX IF NOT EXISTS storage_files_status_idx ON storage_files(status)')
    conn.execute('ALTER TABLE storage_files ENABLE ROW LEVEL SECURITY')
    conn.execute('ALTER TABLE storage_references ENABLE ROW LEVEL SECURITY')
    conn.execute('REVOKE ALL ON storage_files,storage_references FROM PUBLIC')
    conn.execute("""DO $$ BEGIN
      IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='anon') THEN REVOKE ALL ON storage_files,storage_references FROM anon; END IF;
      IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='authenticated') THEN REVOKE ALL ON storage_files,storage_references FROM authenticated; END IF;
      END $$""")


def bounded_copy(source, target):
    digest=hashlib.sha256();size=0
    while True:
        block=source.read(1024*1024)
        if not block:break
        size+=len(block)
        if size>MAX_BYTES:raise ValueError('File exceeds the 25 MB browser-copy limit; keep the Drive original and arrange a separate large-file transfer.')
        digest.update(block);target.write(block)
    target.seek(0)
    return digest.hexdigest(),size


def put_verified(stream, key, mime):
    from boto3.s3.transfer import TransferConfig
    with tempfile.SpooledTemporaryFile(max_size=1024*1024) as spool:
        digest,size=bounded_copy(stream,spool)
        store=client()
        store.upload_fileobj(spool,bucket(),key,ExtraArgs={'ContentType':mime,'Metadata':{'sha256':digest}},
            Config=TransferConfig(use_threads=False,multipart_threshold=8*1024*1024))
        response=store.get_object(Bucket=bucket(),Key=key)
        check=hashlib.sha256();read=0
        try:
            while True:
                block=response['Body'].read(1024*1024)
                if not block:break
                check.update(block);read+=len(block)
                if read>MAX_BYTES:raise ValueError('R2 verification failed.')
        finally:response['Body'].close()
        if read!=size or check.hexdigest()!=digest:raise ValueError('R2 copy did not match the source. The Drive original has not been changed.')
        return digest,size


def upload(file, name=None):
    identity=secrets.token_hex(16);key='erp/files/'+identity
    name=name or file.filename or 'attachment';mime=file.mimetype or 'application/octet-stream'
    digest,size=put_verified(file.stream,key,mime)
    with db() as conn:
        conn.execute("INSERT INTO storage_files(id,name,mime,object_key,sha256,size_bytes,status,created_by,verified_at) VALUES(%s,%s,%s,%s,%s,%s,'verified',%s,now())",
            (identity,name,mime,key,digest,size,session['user']))
    return dict(id='r2:'+identity,name=name,mimeType=mime,webViewLink=url_for('object_storage.open_file',file_id=identity))


def keep_drive_original(reference):
    if enabled() or (reference or '').startswith('r2:'):return True
    if not reference:return False
    with db() as conn:
        return bool(conn.execute('SELECT 1 FROM storage_files WHERE source_id=%s',(reference,)).fetchone())


def rename_reference(conn,reference,name):
    if (reference or '').startswith('r2:'):
        conn.execute('UPDATE storage_files SET name=%s WHERE id=%s',(name,reference[3:]))
    else:
        conn.execute('UPDATE storage_files SET name=%s WHERE source_id=%s',(name,reference))


def virtual_folder(name):
    return dict(id='r2:folder-'+secrets.token_hex(16),name=name,mimeType='application/vnd.google-apps.folder',webViewLink=None)


def allowed(conn, file):
    uid=session.get('uid');username=session.get('user');role=session.get('role')
    # New uploads are bound by their owning ERP row, not possession of the file ID.
    docs=conn.execute('SELECT * FROM doc_items WHERE drive_id=%s',('r2:'+file['id'],)).fetchall()
    from .document_storage import has_item_access_or_inherited
    for row in docs:
        if not current_app.config['HAS_MODULE_ACCESS_FUNC']('DOCUMENT_STORAGE'):continue
        if not row['deleted_at'] and int(row['is_active'])==1 and has_item_access_or_inherited(conn,row,uid,username,role):return True
    messages=conn.execute('SELECT * FROM message_messages WHERE attachment_drive_id=%s AND deleted_at IS NULL',('r2:'+file['id'],)).fetchall()
    from .messages import message_can_access_conversation
    for row in messages:
        if not current_app.config['HAS_MODULE_ACCESS_FUNC']('MESSAGES'):continue
        if message_can_access_conversation(conn,row['conversation_id'],uid):return True
    refs=conn.execute('SELECT * FROM storage_references WHERE file_id=%s',(file['id'],)).fetchall()
    for ref in refs:
        definition=REFERENCES.get(ref['table_name'])
        if not definition or ref['column_name'] not in definition[1]:continue
        row=conn.execute(sql.SQL('SELECT * FROM {} WHERE id=%s').format(sql.Identifier(ref['table_name'])),(ref['row_id'],)).fetchone()
        if not row or row.get('deleted_at') or row.get('is_deleted') or row.get('active')==0:continue
        if row[ref['column_name']]!=ref['original_value']:continue
        if ref['table_name']=='doc_items':
            direct=conn.execute('SELECT * FROM doc_items WHERE drive_id=%s',(file['source_id'],)).fetchall() if file['source_id'] else []
            if direct and not any(not item['deleted_at'] and has_item_access_or_inherited(conn,item,uid,username,role) for item in direct):continue
            if not current_app.config['HAS_MODULE_ACCESS_FUNC']('DOCUMENT_STORAGE'):continue
            if has_item_access_or_inherited(conn,row,uid,username,role):return True
        elif ref['table_name']=='message_messages':
            if not current_app.config['HAS_MODULE_ACCESS_FUNC']('MESSAGES'):continue
            if message_can_access_conversation(conn,row['conversation_id'],uid):return True
        elif ref['table_name']=='users':
            if role=='ADMIN' or row['id']==uid:return True
        elif ref['table_name'].startswith('cash_'):
            # Match the petty-cash API's owner access, including child expenses/topups.
            advance=row if ref['table_name']=='cash_advances' else conn.execute('SELECT * FROM cash_advances WHERE id=%s',(row.get('advance_id'),)).fetchone()
            if role=='ADMIN':return True
            if advance and advance.get('employee_username')==username and current_app.config['HAS_MODULE_ACCESS_FUNC']('CASH_ADVANCES'):return True
        elif current_app.config['HAS_MODULE_ACCESS_FUNC'](definition[0]):return True
    return False


@storage_bp.get('/api/storage/files/<file_id>/open')
def open_file(file_id):
    if 'user' not in session:return jsonify(error='Login required'),401
    with db() as conn:
        active=conn.execute('SELECT id,role FROM users WHERE username=%s AND active=1',(session['user'],)).fetchone()
        if not active or active['id']!=session.get('uid') or active['role']!=session.get('role'):return jsonify(error='Please sign in again'),403
        file=conn.execute('SELECT * FROM storage_files WHERE id=%s',(file_id,)).fetchone()
        if not file or not allowed(conn,file):return jsonify(error='File not found or access denied'),404
        children=conn.execute('SELECT * FROM storage_files WHERE parent_id=%s ORDER BY name',(file_id,)).fetchall() if file['status']=='folder' else []
    if file['status']=='folder':return render_template('storage_folder.html',file=file,children=children,user=session['user'],role=session['role'])
    if file['status']!='verified':
        if file['source_id']:return redirect('https://drive.google.com/open?id='+file['source_id'])
        return jsonify(error='File copy is not ready'),409
    spool=None
    try:
        response=client().get_object(Bucket=bucket(),Key=file['object_key'])
        spool=tempfile.SpooledTemporaryFile(max_size=1024*1024)
        try:
            digest,size=bounded_copy(response['Body'],spool)
            if digest!=file['sha256']:raise ValueError('Stored file checksum mismatch')
        finally:response['Body'].close()
        # Download untrusted documents rather than executing HTML/SVG on the ERP origin.
        result=send_file(spool,mimetype=file['mime'],download_name=file['name'],as_attachment=file['mime'] not in ('application/pdf','image/png','image/jpeg','image/gif','image/webp','text/plain'),conditional=False)
        result.call_on_close(spool.close);result.headers['Cache-Control']='private, no-store';result.headers['X-Content-Type-Options']='nosniff'
        result.headers['Content-Security-Policy']="sandbox; default-src 'none'"
        return result
    except Exception:
        if spool is not None:spool.close()
        current_app.logger.exception('R2 file download unavailable')
        if file['source_id']:return redirect('https://drive.google.com/open?id='+file['source_id'])
        return jsonify(error='File storage is temporarily unavailable. Please retry.'),503


@storage_bp.get('/storage-setup')
@admin_only
def setup_page():return render_template('storage_setup.html',user=session['user'],role='ADMIN')


@storage_bp.get('/api/storage/status')
@admin_only
def status():
    with db() as conn:
        counts=conn.execute('SELECT status,count(*) AS count,coalesce(sum(size_bytes),0) AS bytes FROM storage_files GROUP BY status').fetchall()
    return jsonify(backend='r2' if enabled() else 'drive',files=[dict(r) for r in counts],max_copy_mb=25)


@storage_bp.post('/api/storage/check')
@admin_only
def check():
    try:
        key='erp/connection-check/'+secrets.token_hex(16)
        digest,size=put_verified(io.BytesIO(b'ERP private storage connection test'),key,'text/plain')
        client().delete_object(Bucket=bucket(),Key=key)
        return jsonify(ok=True,message='Private upload, download and checksum verification passed.')
    except Exception:
        current_app.logger.exception('R2 connection test failed')
        return jsonify(ok=False,error='R2 check failed. Check the endpoint, bucket and bucket-scoped Object Read & Write keys in Render.'),503


def add_reference(conn, source, table, row_id, column, original, parent=None):
    identity=hashlib.sha256(source.encode()).hexdigest()[:32]
    conn.execute('INSERT INTO storage_files(id,source_id,parent_id,created_by) VALUES(%s,%s,%s,%s) ON CONFLICT(source_id) DO UPDATE SET parent_id=coalesce(storage_files.parent_id,excluded.parent_id)',(identity,source,parent,session['user']))
    conn.execute('INSERT INTO storage_references VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',(identity,table,row_id,column,original))
    return identity


@storage_bp.post('/api/storage/inventory')
@admin_only
def inventory():
    found=0
    with db() as conn:
        for table,(_,columns) in REFERENCES.items():
            existing={r['column_name'] for r in conn.execute('SELECT column_name FROM information_schema.columns WHERE table_schema=\'public\' AND table_name=%s',(table,)).fetchall()}
            for column in set(columns)&existing:
                rows=conn.execute(sql.SQL('SELECT id,{} AS value FROM {} WHERE {} IS NOT NULL').format(sql.Identifier(column),sql.Identifier(table),sql.Identifier(column))).fetchall()
                for row in rows:
                    source=drive_id(row['value'],raw=column in ('drive_id','attachment_drive_id'))
                    if source:add_reference(conn,source,table,row['id'],column,row['value']);found+=1
    return jsonify(ok=True,references=found,message='ERP references inventoried. Originals are unchanged.')


@storage_bp.post('/api/storage/copy-next')
@admin_only
def copy_next():
    with db() as conn:
        file=conn.execute("SELECT * FROM storage_files WHERE status='pending' OR (status='copying' AND copy_started<now()-interval '5 minutes') ORDER BY created_at,id LIMIT 1 FOR UPDATE SKIP LOCKED").fetchone()
        if file:conn.execute("UPDATE storage_files SET status='copying',copy_started=now() WHERE id=%s",(file['id'],))
    if not file:return jsonify(ok=True,done=True)
    try:
        from .document_storage import get_oauth_drive_service
        from googleapiclient.http import MediaIoBaseDownload
        service=get_oauth_drive_service()
        meta=service.files().get(fileId=file['source_id'],fields='id,name,mimeType,size,md5Checksum,modifiedTime',supportsAllDrives=True).execute()
        mime=meta['mimeType'];name=meta['name']
        if mime=='application/vnd.google-apps.folder':
            result=service.files().list(q="'%s' in parents and trashed=false" % file['source_id'],pageToken=file['scan_page'],pageSize=100,
                fields='nextPageToken,files(id)',supportsAllDrives=True,includeItemsFromAllDrives=True).execute()
            with db() as conn:
                refs=conn.execute('SELECT * FROM storage_references WHERE file_id=%s',(file['id'],)).fetchall()
                for child in result.get('files',[]):
                    for ref in refs:add_reference(conn,child['id'],ref['table_name'],ref['row_id'],ref['column_name'],ref['original_value'],file['id'])
                conn.execute('UPDATE storage_files SET name=%s,mime=%s,status=%s,scan_page=%s WHERE id=%s',(name,mime,'pending' if result.get('nextPageToken') else 'folder',result.get('nextPageToken'),file['id']))
            return jsonify(ok=True,name=name,folder=True)
        if mime.startswith('application/vnd.google-apps.'):
            raise ValueError('Google-native document: retain the editable Drive original; export requires a separate choice.')
        if int(meta.get('size') or 0)>MAX_BYTES:raise ValueError('Larger than 25 MB: retain Drive original; a separate large-file transfer is required.')
        with tempfile.SpooledTemporaryFile(max_size=1024*1024) as spool:
            downloader=MediaIoBaseDownload(spool,service.files().get_media(fileId=file['source_id'],supportsAllDrives=True),chunksize=1024*1024)
            done=False
            while not done:
                _,done=downloader.next_chunk()
                if spool.tell()>MAX_BYTES:raise ValueError('File exceeds browser-copy limit.')
            spool.seek(0);md5=hashlib.md5()
            while True:
                part=spool.read(1024*1024)
                if not part:break
                md5.update(part)
            if meta.get('md5Checksum') and md5.hexdigest()!=meta['md5Checksum']:raise ValueError('Drive file changed while downloading; retry after the file is stable.')
            spool.seek(0);key='erp/migrated/'+file['id'];digest,size=put_verified(spool,key,mime)
            after=service.files().get(fileId=file['source_id'],fields='modifiedTime,md5Checksum',supportsAllDrives=True).execute()
            if after.get('modifiedTime')!=meta.get('modifiedTime'):raise ValueError('Drive file changed during copying; inventory again and retry.')
        with db() as conn:
            conn.execute("UPDATE storage_files SET name=%s,mime=%s,object_key=%s,sha256=%s,size_bytes=%s,status='verified',error='',verified_at=now() WHERE id=%s",(name,mime,key,digest,size,file['id']))
        return jsonify(ok=True,name=name,verified=True)
    except Exception as error:
        if isinstance(error,ValueError):current_app.logger.warning('Drive to R2 copy needs review for file %s: %s',file['id'],error)
        else:current_app.logger.exception('Drive to R2 copy paused for file %s',file['id'])
        message=str(error) if isinstance(error,ValueError) else 'Copy failed. Check Google Drive connection and R2 settings; the original is unchanged.'
        with db() as conn:conn.execute("UPDATE storage_files SET status='review',error=%s WHERE id=%s",(message,file['id']))
        return jsonify(ok=False,error=message,file_id=file['id']),409


@storage_bp.get('/api/storage/review')
@admin_only
def review():
    with db() as conn:rows=conn.execute("SELECT id,source_id,name,error FROM storage_files WHERE status='review' ORDER BY created_at LIMIT 100").fetchall()
    return jsonify(items=[dict(r) for r in rows])


@storage_bp.post('/api/storage/retry')
@admin_only
def retry():
    with db() as conn:conn.execute("UPDATE storage_files SET status='pending',error='' WHERE status='review'")
    return jsonify(ok=True)


def configure_storage(app):
    app.config['FILE_STORAGE_BACKEND']=os.environ.get('FILE_STORAGE_BACKEND','drive').lower()
    app.register_blueprint(storage_bp)
    @app.after_request
    def private_file_links(response):
        # Preserve original DB values for fallback; change only rendered API links.
        if not enabled() or not response.is_json or response.status_code>=400 or request.blueprint=='object_storage':return response
        if request.blueprint not in ('finance','cash_advances','document_storage','messages','users','invoices'):return response
        payload=response.get_json();ids=set()
        def link_field(key):return key.endswith('_link') or key.endswith('_url') or key in ('webViewLink',)
        def collect(value):
            if isinstance(value,dict):
                for k,v in value.items():
                    if isinstance(v,(dict,list)) or link_field(k):collect(v)
            elif isinstance(value,list):
                for v in value:collect(v)
            elif isinstance(value,str):
                identity=drive_id(value)
                if identity:ids.add(identity)
        collect(payload)
        if not ids:return response
        with db() as conn:rows=conn.execute("SELECT id,source_id FROM storage_files WHERE source_id=ANY(%s) AND status IN ('verified','folder')",(list(ids),)).fetchall()
        mapping={r['source_id']:url_for('object_storage.open_file',file_id=r['id']) for r in rows}
        def replace(value):
            if isinstance(value,dict):
                result={k:replace(v) if isinstance(v,(dict,list)) else v for k,v in value.items()}
                links={k:mapping[drive_id(v)] for k,v in value.items() if link_field(k) and isinstance(v,str) and drive_id(v) in mapping}
                if links:result['_storage_links']=links
                return result
            if isinstance(value,list):return [replace(v) for v in value]
            return value
        response.set_data(current_app.json.dumps(replace(payload)));return response


def cleanup_unreferenced_new_file(reference):
    """Only remove R2-only uploads after their owning ERP row is committed deleted."""
    if not (reference or '').startswith('r2:'):return
    identity=reference[3:]
    try:
        with db() as conn:
            if conn.execute('SELECT 1 FROM doc_items WHERE drive_id=%s',(reference,)).fetchone():return
            if conn.execute('SELECT 1 FROM message_messages WHERE attachment_drive_id=%s',(reference,)).fetchone():return
            file=conn.execute('SELECT * FROM storage_files WHERE id=%s AND source_id IS NULL',(identity,)).fetchone()
        if not file:return
        client().delete_object(Bucket=bucket(),Key=file['object_key'])
        with db() as conn:conn.execute('DELETE FROM storage_files WHERE id=%s',(identity,))
    except Exception:
        current_app.logger.exception('R2 orphan cleanup deferred for %s',identity)
