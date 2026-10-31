import hashlib
import io
import os
import unittest
from unittest.mock import Mock,patch
from pathlib import Path
from flask import Flask
from routes.object_storage import bounded_copy,drive_id,client,REFERENCES,configure_storage

class StorageUnitTests(unittest.TestCase):
    def test_google_ids_are_not_arbitrary_urls(self):
        self.assertEqual(drive_id('https://drive.google.com/file/d/valid_google_id_123/view'),'valid_google_id_123')
        self.assertEqual(drive_id('https://drive.google.com/drive/folders/valid_google_id_123'),'valid_google_id_123')
        for value in ('https://evil.example/file/d/valid_google_id_123','https://drive.google.com.evil.example/?id=valid_google_id_123','r2:abc','javascript:alert(1)'):
            self.assertIsNone(drive_id(value))
    def test_streaming_limit_and_digest(self):
        with io.BytesIO() as target:
            digest,size=bounded_copy(io.BytesIO(b'exact bytes'),target)
            self.assertEqual(digest,hashlib.sha256(b'exact bytes').hexdigest());self.assertEqual(size,11)
        with patch('routes.object_storage.MAX_BYTES',2):
            with self.assertRaises(ValueError):bounded_copy(io.BytesIO(b'abc'),io.BytesIO())
    def test_settings_remain_drive_until_explicit_switch(self):
        app=Flask(__name__)
        with patch.dict(os.environ,{'FILE_STORAGE_BACKEND':'drive'}):configure_storage(app)
        self.assertEqual(app.config['FILE_STORAGE_BACKEND'],'drive')
        with app.app_context(),patch.dict(os.environ,{},clear=True):
            with self.assertRaises(ValueError):client()
    def test_bad_endpoint_never_receives_access_keys(self):
        app=Flask(__name__);app.config.update(R2_BUCKET='test',R2_ENDPOINT_URL='https://evil.example',R2_ACCESS_KEY_ID='private',R2_SECRET_ACCESS_KEY='private')
        with app.app_context():
            with self.assertRaises(ValueError):client()

DSN=os.environ.get('ERP_R2_TEST_DSN')
@unittest.skipUnless(DSN,'Requires disposable local PostgreSQL plus moto')
class StorageIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from psycopg.conninfo import conninfo_to_dict
        info=conninfo_to_dict(DSN);assert info.get('host')=='127.0.0.1' and info.get('dbname')=='erp_audit_smoke'
        os.environ.update(DATABASE_URL=DSN,SECRET_KEY='isolated-r2-test',BOOTSTRAP_ADMIN_USERNAME='AuditAdmin',BOOTSTRAP_ADMIN_PASSWORD='Isolated-test-only-8912')
        from app import app
        cls.app=app;app.config.update(TESTING=True,SESSION_COOKIE_SECURE=False,FILE_STORAGE_BACKEND='r2',R2_BUCKET='test-erp')
    def setUp(self):
        from moto import mock_aws
        import boto3
        self.mock=mock_aws();self.mock.start()
        self.s3=boto3.client('s3',region_name='us-east-1');self.s3.create_bucket(Bucket='test-erp')
        from routes.db import connect
        with connect(DSN) as conn:
            conn.execute('DELETE FROM storage_references');conn.execute('DELETE FROM storage_files')
        self.patch=patch('routes.object_storage.client',return_value=self.s3);self.patch.start()
        from app import RATE_BUCKETS
        RATE_BUCKETS.clear()
        self.client=self.app.test_client();self.client.environ_base['HTTP_X_ERP_STORAGE']='1';self.client.get('/login')
        self.assertEqual(self.client.post('/login',data={'username':'AuditAdmin','password':'Isolated-test-only-8912'}).status_code,302)
    def tearDown(self):self.patch.stop();self.mock.stop()
    def test_connection_private_upload_download_and_access_revocation(self):
        from routes.db import connect
        check=self.client.post('/api/storage/check');self.assertEqual(check.status_code,200,check.json)
        self.assertEqual(self.s3.list_objects_v2(Bucket='test-erp').get('KeyCount'),0)
        with patch('routes.document_storage.get_oauth_drive_service',side_effect=AssertionError('R2 must not contact Drive')):
            response=self.client.post('/api/docs/upload',data={'name':'Exact proof.txt','file':(io.BytesIO(b'original file bytes'),'proof.txt')})
        self.assertEqual(response.status_code,200,response.json)
        row=response.json['data'];self.assertTrue(row['drive_id'].startswith('r2:'))
        download=self.client.get(row['web_view_link']);self.assertEqual(download.status_code,200);self.assertEqual(download.data,b'original file bytes');self.assertIn('inline;',download.headers['Content-Disposition']);download.close()
        guest=self.app.test_client();self.assertEqual(guest.get(row['web_view_link']).status_code,401)
        self.assertEqual(self.client.put('/api/docs/items/'+str(row['id']),json={'name':'Renamed proof.txt'}).status_code,200)
        response=self.client.get(row['web_view_link']);self.assertIn('Renamed proof.txt',response.headers['Content-Disposition']);response.close()
        self.assertEqual(self.client.delete('/api/docs/items/'+str(row['id'])).status_code,200)
        self.assertEqual(self.client.get(row['web_view_link']).status_code,404)
        self.assertEqual(self.client.post('/api/docs/items/'+str(row['id'])+'/restore').status_code,200)
        response=self.client.get(row['web_view_link']);self.assertEqual(response.status_code,200);response.close()
        self.client.delete('/api/docs/items/'+str(row['id']))
        self.assertEqual(self.client.post('/api/docs/items/'+str(row['id'])+'/delete-forever').status_code,200)
        self.assertEqual(self.s3.list_objects_v2(Bucket='test-erp').get('KeyCount'),0)
    def test_verified_copy_preserves_database_links_and_edit_payload(self):
        from routes.db import connect
        from routes.object_storage import add_reference
        source='drive_file_for_migration_123'
        original='https://drive.google.com/file/d/'+source+'/view'
        with self.app.test_request_context('/migration'):
            from flask import session
            session.update(user='AuditAdmin')
            with connect(DSN) as conn:
                bank=conn.execute("SELECT id FROM bank_accounts LIMIT 1").fetchone()['id']
                row=conn.execute("INSERT INTO finance_records(bank_id,type,client_name,category,description,currency,amount,payment_type,status,proof_of_payment,proof_link,created_at,created_by) VALUES(%s,'OUTCOME','Preview','Transport','Exact description','LKR',12,'BANK','PAID','OPEN',%s,'2026-11-01','AuditAdmin') RETURNING id",(bank,original)).fetchone()['id']
                file_id=add_reference(conn,source,'finance_records',row,'proof_link',original)
                conn.execute("UPDATE storage_files SET name='proof',object_key='erp/test-copy',status='verified',sha256=%s WHERE id=%s",(hashlib.sha256(b'copied').hexdigest(),file_id))
        self.s3.put_object(Bucket='test-erp',Key='erp/test-copy',Body=b'copied')
        response=self.client.get('/api/finance?bank_id=ALL');self.assertEqual(response.status_code,200)
        record=next(r for r in response.json if r['id']==row)
        self.assertEqual(record['proof_link'],original);self.assertIn('_storage_links',record)
        private=record['_storage_links']['proof_link'];response=self.client.get(private);self.assertEqual(response.data,b'copied');response.close()
        self.client.put('/api/finance/'+str(row),json={'description':'Edited description','proof_link':record['proof_link']})
        response=self.client.get(private);self.assertEqual(response.status_code,200);response.close()
        with connect(DSN) as conn:
            self.assertEqual(conn.execute('SELECT proof_link FROM finance_records WHERE id=%s',(row,)).fetchone()['proof_link'],original)
    def test_copy_reads_back_and_never_changes_the_drive_original(self):
        from routes.db import connect
        from routes.object_storage import add_reference
        from flask import session
        source='copy_google_file_'+os.urandom(5).hex()
        with self.app.test_request_context('/migration'):
            session.update(user='AuditAdmin')
            with connect(DSN) as conn:file_id=add_reference(conn,source,'finance_records',1,'proof_link','https://drive.google.com/open?id='+source)
        content=b'exact source bytes'
        drive=Mock();drive.files.return_value.get.return_value.execute.return_value={'id':source,'name':'proof.txt','mimeType':'text/plain','size':str(len(content)),'md5Checksum':hashlib.md5(content).hexdigest(),'modifiedTime':'stable'}
        class Downloader:
            def __init__(self,stream,*a,**k):self.stream=stream
            def next_chunk(self):self.stream.write(content);return None,True
        with patch('routes.document_storage.get_oauth_drive_service',return_value=drive),patch('googleapiclient.http.MediaIoBaseDownload',Downloader):
            response=self.client.post('/api/storage/copy-next')
        self.assertEqual(response.status_code,200,response.json)
        with connect(DSN) as conn:
            row=conn.execute('SELECT * FROM storage_files WHERE id=%s',(file_id,)).fetchone()
            self.assertEqual(row['status'],'verified');self.assertEqual(row['sha256'],hashlib.sha256(content).hexdigest())
        drive.files.return_value.delete.assert_not_called();drive.files.return_value.update.assert_not_called()
        self.assertEqual(self.s3.get_object(Bucket='test-erp',Key=row['object_key'])['Body'].read(),content)

    def test_google_native_file_is_reviewed_not_silently_converted(self):
        from routes.db import connect
        from routes.object_storage import add_reference
        from flask import session
        with self.app.test_request_context('/migration'):
            session.update(user='AuditAdmin')
            with connect(DSN) as conn:file_id=add_reference(conn,'native_google_document_123','finance_records',1,'proof_link','original')
        drive=Mock();drive.files.return_value.get.return_value.execute.return_value={'name':'Editable document','mimeType':'application/vnd.google-apps.document'}
        with patch('routes.document_storage.get_oauth_drive_service',return_value=drive):response=self.client.post('/api/storage/copy-next')
        self.assertEqual(response.status_code,409)
        with connect(DSN) as conn:
            row=conn.execute('SELECT * FROM storage_files WHERE id=%s',(file_id,)).fetchone();self.assertEqual(row['status'],'review')
        self.assertEqual(self.s3.list_objects_v2(Bucket='test-erp').get('KeyCount'),0)

    def test_message_attachment_uses_r2_and_conversation_permissions(self):
        response=self.client.post('/api/messages/groups',json={'title':'Storage test','member_user_ids':[]})
        self.assertEqual(response.status_code,200,response.json)
        cid=response.json['data']['id']
        with patch('routes.messages.get_oauth_drive_service',side_effect=AssertionError('R2 attachment must not use Drive')):
            response=self.client.post('/api/messages/conversations/'+str(cid)+'/messages',data={'file':(io.BytesIO(b'private attachment'),'attachment.txt')})
        self.assertEqual(response.status_code,200,response.json)
        item=response.json['data'];self.assertTrue(item['attachment_drive_id'].startswith('r2:'))
        response=self.client.get(item['attachment_url'],follow_redirects=True)
        self.assertEqual(response.status_code,200);self.assertEqual(response.data,b'private attachment');response.close()
        guest=self.app.test_client();self.assertEqual(guest.get(item['attachment_url']).status_code,401)

    def test_folder_pages_are_queued_with_inherited_erp_references(self):
        from routes.db import connect
        from routes.object_storage import add_reference
        from flask import session
        with self.app.test_request_context('/migration'):
            session.update(user='AuditAdmin')
            with connect(DSN) as conn:identity=add_reference(conn,'google_folder_123','finance_records',1,'folder_link','original')
        drive=Mock();drive.files.return_value.get.return_value.execute.return_value={'name':'ERP folder','mimeType':'application/vnd.google-apps.folder'}
        drive.files.return_value.list.return_value.execute.return_value={'files':[{'id':'google_child_file_123'}]}
        with patch('routes.document_storage.get_oauth_drive_service',return_value=drive):response=self.client.post('/api/storage/copy-next')
        self.assertEqual(response.status_code,200,response.json)
        with connect(DSN) as conn:
            parent=conn.execute('SELECT * FROM storage_files WHERE id=%s',(identity,)).fetchone()
            child=conn.execute('SELECT * FROM storage_files WHERE parent_id=%s',(identity,)).fetchone()
            self.assertEqual(parent['status'],'folder');self.assertEqual(child['status'],'pending')
            self.assertTrue(conn.execute('SELECT 1 FROM storage_references WHERE file_id=%s',(child['id'],)).fetchone())

    def test_cross_origin_storage_changes_are_rejected(self):
        self.assertEqual(self.client.post('/api/storage/check',headers={'Origin':'https://untrusted.example'}).status_code,403)

    def test_employee_file_access_tracks_current_record_permissions(self):
        from routes.db import connect
        from werkzeug.security import generate_password_hash
        username='storage_emp_'+os.urandom(4).hex()
        response=self.client.post('/api/docs/upload',data={'file':(io.BytesIO(b'restricted proof'),'proof.txt')})
        item=response.json['data']
        with connect(DSN) as conn:
            uid=conn.execute("INSERT INTO users(username,password_hash,role,active,created_at,created_by) VALUES(%s,%s,'EMP',1,'2026-11-01','AuditAdmin') RETURNING id",(username,generate_password_hash('Local-test-only'))).fetchone()['id']
            conn.execute("INSERT INTO user_permissions(user_id,module,can_access,can_edit) VALUES(%s,'DOCUMENT_STORAGE',1,0)",(uid,))
            conn.execute('INSERT INTO doc_item_permissions(item_id,user_id,can_access,can_edit) VALUES(%s,%s,1,0)',(item['id'],uid))
        employee=self.app.test_client();employee.get('/login')
        self.assertEqual(employee.post('/login',data={'username':username,'password':'Local-test-only'}).status_code,302)
        result=employee.get(item['web_view_link']);self.assertEqual(result.status_code,200);result.close()
        with connect(DSN) as conn:conn.execute('UPDATE doc_item_permissions SET can_access=0 WHERE item_id=%s AND user_id=%s',(item['id'],uid))
        self.assertEqual(employee.get(item['web_view_link']).status_code,404)

    def test_inventoried_original_is_not_deleted_in_drive_mode(self):
        from routes.db import connect
        from routes.object_storage import add_reference
        from routes.document_storage import drive_delete_item_safe
        from flask import session
        with self.app.test_request_context('/migration'),patch.dict(self.app.config,{'FILE_STORAGE_BACKEND':'drive'}):
            session.update(user='AuditAdmin')
            with connect(DSN) as conn:add_reference(conn,'protected_drive_original_123','finance_records',1,'proof_link','original')
            with patch('routes.document_storage.get_oauth_drive_service',side_effect=AssertionError('Original must not be deleted')):
                self.assertFalse(drive_delete_item_safe('protected_drive_original_123')['deleted_in_drive'])

    def test_admin_management_requires_live_admin_role(self):
        guest=self.app.test_client()
        for path in ['/api/storage/status','/api/storage/review','/storage-setup']:
            self.assertEqual(guest.get(path).status_code,401)
        with self.client.session_transaction() as s:values=dict(s)
        with guest.session_transaction() as s:s.update(values);s['role']='EMP'
        self.assertEqual(guest.post('/api/storage/check').status_code,403)
