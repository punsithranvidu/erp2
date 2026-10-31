(() => {
 'use strict';
 const el=id=>document.getElementById(id), form=el('activityFilters');
 const dates=new Intl.DateTimeFormat('en-GB',{timeZone:'Asia/Colombo',dateStyle:'medium',timeStyle:'medium'});
 let next=null, controller=null, detailRequest=null;
 const node=(tag,text)=>{const item=document.createElement(tag);if(text!==undefined)item.textContent=text;return item;};
 const api=async(url,signal)=>{const response=await fetch(url,{signal});const data=await response.json();if(!response.ok)throw new Error(data.error||'Activity is unavailable. Please retry.');return data;};
 const when=value=>{const date=new Date(value);return Number.isNaN(date.getTime())?value:dates.format(date);};
 const printable=value=>value===null||value===undefined?'—':typeof value==='object'?JSON.stringify(value,null,2):String(value);
 const label=key=>key.replaceAll('_',' ').replace(/\b\w/g,c=>c.toUpperCase());
 const details=async id=>{
  if(detailRequest)detailRequest.abort();detailRequest=new AbortController();
  const dialog=el('activityDetail'), box=el('activityDetailBody');box.replaceChildren(node('p','Loading details…'));dialog.showModal();
  try {
   const data=await api('/api/activity-log/'+id,detailRequest.signal);if(!dialog.open)return;
   const meta=node('div');meta.className='activity-meta';
   [['Date & time',when(data.occurred_at)],['User',data.actor],['Module',data.module],['Action',data.action],['Record',data.record_key||'—'],['Record type',label(data.entity)],['Operation reference',data.request_id||'System'],['Request',data.path||'Database / system'],['IP address',data.ip_address||'—'],['Browser',data.browser||'—']].forEach(([key,value])=>{const entry=node('div');entry.append(node('strong',key),node('span',value));meta.append(entry);});
   const table=node('table');table.className='table activity-diff';const head=node('thead'), headings=node('tr');['Field','Before','After'].forEach(text=>headings.append(node('th',text)));head.append(headings);table.append(head);const body=node('tbody');
   const changes=data.changes||{};
   const snapshot=changes.after||changes.before;
   const fields=snapshot&&typeof snapshot==='object'?Object.entries(snapshot).map(([key,value])=>[key,changes.before?value:null,changes.after?value:null]):Object.entries(changes).map(([key,value])=>[key,value?.before??null,value&&typeof value==='object'&&Object.hasOwn(value,'after')?value.after:value]);
   fields.forEach(([key,before,after])=>{const tr=node('tr');tr.append(node('td',label(key)),node('td',printable(before)),node('td',printable(after)));body.append(tr);});table.append(body);
   const wrap=node('div');wrap.className='tableWrap';wrap.append(table);box.replaceChildren(meta,wrap);
  } catch(error){if(error.name!=='AbortError')box.replaceChildren(node('p',error.message));}
 };
 const load=async append=>{
  if(controller)controller.abort();controller=new AbortController();el('activityMore').disabled=true;
  if(!append){el('activityRows').replaceChildren();next=null;el('activityMore').hidden=true;el('activityCount').textContent='';}el('activityMessage').textContent='Loading activity…';
  const params=new URLSearchParams(new FormData(form));if(append&&next)params.set('before',next);
  try {
   const data=await api('/api/activity-log?'+params,controller.signal);
   data.items.forEach(item=>{const tr=node('tr');[when(item.occurred_at),item.actor,item.module,item.action,item.record_key?'#'+item.record_key:'—'].forEach(value=>tr.append(node('td',value)));const cell=node('td'),button=node('button','View details');button.type='button';button.className='btn ghost mini';button.setAttribute('aria-label',`View details for ${item.module} ${item.action}, ${item.actor}, ${when(item.occurred_at)}`);button.addEventListener('click',()=>details(item.id));cell.append(button);tr.append(cell);el('activityRows').append(tr);});
   next=data.next_cursor;el('activityMore').hidden=!next;el('activityCount').textContent=`${el('activityRows').children.length} entries shown · newest first`;el('activityMessage').textContent=data.items.length?'':'No activity matches these filters.';
  } catch(error){if(error.name!=='AbortError')el('activityMessage').textContent=error.message;}
  finally{el('activityMore').disabled=false;}
 };
 el('activityDetail').addEventListener('close',()=>{if(detailRequest)detailRequest.abort();});
 el('activityMonth').addEventListener('change',()=>{
  const month=el('activityMonth').value;if(!month)return;
  const [year,number]=month.split('-').map(Number);
  el('activityFrom').value=month+'-01';
  el('activityTo').value=month+'-'+String(new Date(Date.UTC(year,number,0)).getUTCDate()).padStart(2,'0');
 });
 ['activityFrom','activityTo'].forEach(id=>el(id).addEventListener('change',()=>{el('activityMonth').value='';}));
 form.addEventListener('submit',event=>{event.preventDefault();if(form.reportValidity())load(false);});
 el('activityReset').addEventListener('click',()=>{form.reset();load(false);});el('activityMore').addEventListener('click',()=>load(true));el('activityClose').addEventListener('click',()=>el('activityDetail').close());
 api('/api/activity-log/options').then(data=>{
  [['activityUser',data.users],['activityModule',data.modules],['activityAction',data.actions]].forEach(([id,values])=>values.forEach(value=>el(id).add(new Option(value,value))));
  el('activityStorage').textContent=`Activity log storage, including indexes: ${(data.storage_bytes/1048576).toFixed(2)} MB. This is part of your database usage.`;
  el('activityCoverage').textContent=`Database tables currently covered: ${data.covered_tables.length}. Module tables created on first use are added automatically.`;
 }).catch(error=>{el('activityStorage').textContent=error.message;});
 load(false);
})();
