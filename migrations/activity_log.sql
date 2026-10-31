-- Appended in the same transaction as each row change. No public/REST access.
CREATE TABLE IF NOT EXISTS public.erp_activity_log (
 id BIGSERIAL PRIMARY KEY,
 occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 actor TEXT NOT NULL, actor_id TEXT NOT NULL DEFAULT '',
 module TEXT NOT NULL, action TEXT NOT NULL, entity TEXT NOT NULL,
 record_key TEXT NOT NULL DEFAULT '', request_id TEXT NOT NULL DEFAULT '',
 path TEXT NOT NULL DEFAULT '', ip_address TEXT NOT NULL DEFAULT '', browser TEXT NOT NULL DEFAULT '',
 changes JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS erp_activity_time_idx ON public.erp_activity_log(occurred_at);
CREATE INDEX IF NOT EXISTS erp_activity_actor_idx ON public.erp_activity_log(actor,id DESC);
CREATE INDEX IF NOT EXISTS erp_activity_module_idx ON public.erp_activity_log(module,id DESC);
ALTER TABLE public.erp_activity_log ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.erp_activity_log FROM PUBLIC;
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='anon') THEN REVOKE ALL ON public.erp_activity_log FROM anon; END IF;
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='authenticated') THEN REVOKE ALL ON public.erp_activity_log FROM authenticated; END IF;
END $$;

CREATE OR REPLACE FUNCTION public.erp_audit_safe(value JSONB) RETURNS JSONB
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,public AS $$
DECLARE result JSONB; pair RECORD; BEGIN
 IF jsonb_typeof(value)='object' THEN
  result='{}';
  FOR pair IN SELECT * FROM jsonb_each(value) LOOP
   result=result || jsonb_build_object(pair.key,CASE WHEN pair.key ~* '(password|passwd|secret|token|credential|session_id|authorization|api_key|private_key|code_verifier)'
    THEN '"[REDACTED]"'::jsonb ELSE public.erp_audit_safe(pair.value) END);
  END LOOP;
  RETURN result;
 ELSIF jsonb_typeof(value)='array' THEN
  SELECT coalesce(jsonb_agg(public.erp_audit_safe(v)),'[]') INTO result FROM jsonb_array_elements(value) v;
  RETURN result;
 END IF;
 RETURN value;
END $$;

CREATE OR REPLACE FUNCTION public.erp_capture_activity() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE old_row JSONB='{}'; new_row JSONB='{}'; details JSONB='{}'; key_name TEXT; label TEXT; identity TEXT; p TEXT;
BEGIN
 IF TG_OP<>'INSERT' THEN old_row=to_jsonb(OLD); END IF;
 IF TG_OP<>'DELETE' THEN new_row=to_jsonb(NEW); END IF;
 IF TG_OP='UPDATE' THEN
  FOR key_name IN SELECT jsonb_object_keys(new_row) LOOP
   IF new_row->key_name IS DISTINCT FROM old_row->key_name THEN
    details=details || jsonb_build_object(key_name,jsonb_build_object('before',public.erp_audit_safe(jsonb_build_object(key_name,old_row->key_name))->key_name,'after',public.erp_audit_safe(jsonb_build_object(key_name,new_row->key_name))->key_name));
   END IF;
  END LOOP;
  IF details='{}'::jsonb THEN RETURN NEW; END IF;
  label='Edited';
  IF (nullif(old_row->>'deleted_at','') IS NULL AND nullif(new_row->>'deleted_at','') IS NOT NULL)
    OR (coalesce(old_row->>'is_deleted','0') IN ('0','false') AND new_row->>'is_deleted' IN ('1','true')) THEN label='Deleted';
    details=details || jsonb_build_object('_record_snapshot',jsonb_build_object('before',public.erp_audit_safe(old_row),'after',null));
  ELSIF (nullif(old_row->>'deleted_at','') IS NOT NULL AND nullif(new_row->>'deleted_at','') IS NULL)
    OR (old_row->>'is_deleted' IN ('1','true') AND new_row->>'is_deleted' IN ('0','false')) THEN label='Restored'; END IF;
 ELSE
  label=CASE WHEN TG_OP='INSERT' THEN 'Created' ELSE 'Deleted' END;
  details=jsonb_build_object(CASE WHEN TG_OP='INSERT' THEN 'after' ELSE 'before' END,public.erp_audit_safe(CASE WHEN TG_OP='INSERT' THEN new_row ELSE old_row END));
 END IF;
 identity=coalesce(new_row->>'id',old_row->>'id',new_row->>'setting_key',old_row->>'setting_key','');
 IF identity='' THEN
  SELECT coalesce(string_agg(k||'='||v,', ' ORDER BY k),'') INTO identity
   FROM jsonb_each_text(CASE WHEN TG_OP='DELETE' THEN old_row ELSE new_row END) e(k,v)
   WHERE k IN ('user_id','module','item_id','conversation_id','bank_id','month_key','currency','achievement_id');
 END IF;
 p=coalesce(current_setting('erp.audit_path',true),'');
 INSERT INTO public.erp_activity_log(actor,actor_id,module,action,entity,record_key,request_id,path,ip_address,browser,changes)
 VALUES(coalesce(nullif(current_setting('erp.audit_actor',true),''),'System'),coalesce(current_setting('erp.audit_uid',true),''),TG_ARGV[0],label,TG_TABLE_NAME,identity,
 coalesce(current_setting('erp.audit_request',true),''),p,coalesce(current_setting('erp.audit_ip',true),''),coalesce(current_setting('erp.audit_agent',true),''),details);
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION public.erp_protect_activity() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$ BEGIN
 IF TG_OP='DELETE' AND OLD.occurred_at < clock_timestamp()-interval '90 days' THEN RETURN OLD; END IF;
 RAISE EXCEPTION 'Activity history is append-only; only expired entries may be removed.' USING ERRCODE='42501';
END $$;
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='public.erp_activity_log'::regclass AND tgname='erp_activity_immutable') THEN
  CREATE TRIGGER erp_activity_immutable BEFORE UPDATE OR DELETE ON public.erp_activity_log FOR EACH ROW EXECUTE FUNCTION public.erp_protect_activity();
  CREATE TRIGGER erp_activity_no_truncate BEFORE TRUNCATE ON public.erp_activity_log FOR EACH STATEMENT EXECUTE FUNCTION public.erp_protect_activity();
 END IF;
END $$;

CREATE OR REPLACE FUNCTION public.erp_prune_activity() RETURNS INTEGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$ DECLARE removed INTEGER; BEGIN
 DELETE FROM public.erp_activity_log WHERE id IN (SELECT id FROM public.erp_activity_log WHERE occurred_at<clock_timestamp()-interval '90 days' ORDER BY occurred_at LIMIT 2000);
 GET DIAGNOSTICS removed=ROW_COUNT;
 RETURN removed;
END $$;
REVOKE ALL ON FUNCTION public.erp_capture_activity() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.erp_prune_activity() FROM PUBLIC;
-- Supabase may have explicit default grants in addition to PUBLIC privileges.
REVOKE ALL ON SEQUENCE public.erp_activity_log_id_seq FROM PUBLIC;
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='anon') THEN
  REVOKE ALL ON SEQUENCE public.erp_activity_log_id_seq FROM anon;
  REVOKE ALL ON FUNCTION public.erp_capture_activity(),public.erp_prune_activity(),public.erp_protect_activity(),public.erp_audit_safe(JSONB) FROM anon;
 END IF;
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='authenticated') THEN
  REVOKE ALL ON SEQUENCE public.erp_activity_log_id_seq FROM authenticated;
  REVOKE ALL ON FUNCTION public.erp_capture_activity(),public.erp_prune_activity(),public.erp_protect_activity(),public.erp_audit_safe(JSONB) FROM authenticated;
 END IF;
END $$;
