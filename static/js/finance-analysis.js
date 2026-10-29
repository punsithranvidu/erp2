(() => {
  'use strict';
  const el = id => document.getElementById(id);
  const NS = 'http://www.w3.org/2000/svg';
  let metadata = null, report = null, request = null, legacy = null;
  const hiddenAmounts = new Set();
  try { JSON.parse(sessionStorage.getItem('finance.hiddenAmounts') || '[]').forEach(label => hiddenAmounts.add(label)); } catch (_) {}
  const money = value => new Intl.NumberFormat('en-GB', {minimumFractionDigits:2, maximumFractionDigits:2}).format(value);
  const svgNode = (name, attrs={}, text=null) => {
    const node = document.createElementNS(NS,name);
    Object.entries(attrs).forEach(([key,value])=>node.setAttribute(key,String(value)));
    if(text!==null)node.textContent=text;
    return node;
  };
  const option = (select,label,value) => select.add(new Option(label,value));
  const definitions = () => el('type').value==='INCOME' ? metadata.income : metadata.expense;
  const custom = () => {
    const group=el('categoryGroup').value, child=el('categoryChild').value;
    el('customCategoryFields').hidden = group!=='__custom__' && child!=='__custom__';
    el('customCategoryGroup').value = group!=='__custom__' && group!=='__legacy__' ? group : el('customCategoryGroup').value;
    el('customCategoryGroup').readOnly = group!=='__custom__';
    el('customCategoryGroup').required = !el('customCategoryFields').hidden;
    el('customCategoryChild').required = child==='__custom__' && group!=='__custom__';
  };
  const children = chosen => {
    const group=el('categoryGroup').value, select=el('categoryChild');select.replaceChildren();
    if (!group) { option(select,'No subcategory',''); custom(); return; }
    if(group==='__legacy__') { option(select,'Keep existing category','__legacy__'); }
    else if(group==='__custom__') { option(select,'Custom subcategory','__custom__'); }
    else { option(select,'No subcategory',''); (definitions()[group]||[]).forEach(name=>option(select,name,name));option(select,'Add custom subcategory…','__custom__'); }
    if(chosen && [...select.options].some(item=>item.value===chosen)) select.value=chosen;
    custom();
  };
  const populate = (value=null) => {
    if(!metadata)return;
    const select=el('categoryGroup'); select.replaceChildren();option(select,'Choose category','');
    Object.keys(definitions()).forEach(name=>option(select,name,name));
    option(select,'Add custom category…','__custom__');
    legacy=null;el('customCategoryGroup').value='';el('customCategoryChild').value='';el('legacyCategoryNote').hidden=true;
    if(value){
      const parts=value.split(' / '), group=parts.shift(), child=parts.join(' / ');
      if(definitions()[group]) {select.value=group;children(child);if(child && !definitions()[group].includes(child)){el('categoryChild').value='__custom__';el('customCategoryChild').value=child;custom();}}
      else {legacy=value;option(select,'Keep existing: '+value,'__legacy__');select.value='__legacy__';children();el('legacyCategoryNote').textContent='Existing category is preserved. Choose a new category only if you want to reclassify this record.';el('legacyCategoryNote').hidden=false;}
    } else children();
    el('category').value=value||'';
  };
  const categoryValue = () => {
    if(el('categoryGroup').value==='__legacy__')return legacy;
    const group=el('categoryGroup').value==='__custom__'?el('customCategoryGroup').value.trim():el('categoryGroup').value;
    const child=el('categoryChild').value==='__custom__'?el('customCategoryChild').value.trim():el('categoryChild').value;
    if(!group||group.includes(' / ')||child.includes(' / ')||(el('categoryChild').value==='__custom__'&&el('categoryGroup').value!=='__custom__'&&!child))throw new Error('Choose a category without the separator “ / ”.');
    return child ? `${group} / ${child}` : group;
  };
  const chart = (target, line=false) => {
    const box=el(target);box.replaceChildren();
    const rows=report.monthly;
    if(!rows.length){box.textContent='No records match these filters.';box.className='finance-chart-empty';return;}
    box.className='';
    const width=Math.max(520,rows.length*40+70),height=280,left=65,right=20,top=32,bottom=55;
    const w=width-left-right,h=height-top-bottom,max=Math.max(1,...rows.map(r=>Math.max(r.income,line?0:r.outgoing)))*1.12;
    const svg=svgNode('svg',{viewBox:`0 0 ${width} ${height}`,role:'img','aria-label':line?`Monthly income in ${report.currency}`:`Monthly income and expense amounts in ${report.currency}`});
    svg.append(svgNode('title',{},line?'Income over time':'Income versus expenses'));
    for(let i=0;i<=4;i++){
      const y=top+h-h*i/4;svg.append(svgNode('line',{x1:left,x2:width-right,y1:y,y2:y,stroke:'#e5ebf2'}));
      svg.append(svgNode('text',{x:left-8,y:y+4,'text-anchor':'end',fill:'#7a889a','font-size':10},new Intl.NumberFormat('en',{notation:'compact',maximumFractionDigits:1}).format(max*i/4)));
    }
    const slot=w/rows.length,points=[];
    rows.forEach((row,index)=>{
      const x=left+slot*(index+.5), y=top+h-row.income/max*h;
      const label=new Intl.DateTimeFormat('en',{month:'short',year:'2-digit'}).format(new Date(row.period+'-01T12:00:00'));
      if(index%Math.max(1,Math.ceil(rows.length/10))===0)svg.append(svgNode('text',{x,y:height-29,'text-anchor':'middle',fill:'#78879a','font-size':10},label));
      if(line){points.push(`${x},${y}`);const dot=svgNode('circle',{cx:x,cy:y,r:3,fill:'#315f91'});dot.append(svgNode('title',{},`${row.period}: ${report.currency} ${money(row.income)}`));svg.append(dot);}
      else [['income','#315f91',-1],['outgoing','#a77c64',1]].forEach(([key,color,side])=>{
        const bw=Math.min(12,slot/3),bh=row[key]/max*h;
        const bar=svgNode('rect',{x:x+(side===-1?-bw-1:1),y:top+h-bh,width:bw,height:bh,rx:1,fill:color});bar.append(svgNode('title',{},`${row.period} ${key==='outgoing'?'Expenses':'Income'}: ${report.currency} ${money(row[key])}`));svg.append(bar);
      });
    });
    if(line)svg.insertBefore(svgNode('polyline',{points:points.join(' '),fill:'none',stroke:'#315f91','stroke-width':2}),svg.children[10]||null);
    svg.append(svgNode('rect',{x:left,y:height-15,width:9,height:9,fill:'#315f91'}));
    svg.append(svgNode('text',{x:left+15,y:height-6,fill:'#65758b','font-size':11},'Income · '+report.currency));
    if (!line) { svg.append(svgNode('rect',{x:left+130,y:height-15,width:9,height:9,fill:'#a77c64'}));svg.append(svgNode('text',{x:left+145,y:height-6,fill:'#65758b','font-size':11},'Expenses')); }
    box.append(svg);
  };
  const breakdown = (group=null) => {
    const box=el('financeExpenseChart');box.replaceChildren();
    el('expenseBack').hidden=!group;
    el('expenseChartTitle').textContent=(group?group.name+' — subcategories':'Expenses by category')+' · '+report.currency;
    const rows=group?group.children:report.categories;
    if(!rows.length){box.textContent='No expense records match these filters.';return;}
    const max=Math.max(1,...rows.map(row=>row.amount));
    rows.forEach(row=>{
      const button=document.createElement(group?'div':'button');button.className='finance-category-row';if(!group)button.type='button';
      const name=document.createElement('span');name.textContent=row.name||'No subcategory';
      const track=document.createElement('span');track.className='finance-category-track';track.setAttribute('aria-hidden','true');
      const fill=document.createElement('span');fill.className='finance-category-fill';fill.style.display='block';fill.style.width=(row.amount/max*100)+'%';track.append(fill);
      const value=document.createElement('strong');value.textContent=money(row.amount);
      button.append(name,track,value);if(!group){button.setAttribute('aria-label',`${row.name}, ${report.currency} ${money(row.amount)}. Show subcategories`);button.addEventListener('click',()=>breakdown(row));}box.append(button);
    });
  };
  const render = () => {
    const kpis=el('analysisKpis');kpis.replaceChildren();
    [['Income',report.income,report.currency],['Expenses',report.outgoing,report.currency],['Net movement',report.net_movement,'Income minus expenses; not profit'],['Records',report.count,`${report.unsettled_count} not marked received / paid`]].forEach(([label,amount,note])=>{
      const item=document.createElement('div');item.className='finance-kpi';
      const title=document.createElement('span');title.textContent=label;
      const value=document.createElement('strong');value.textContent=label==='Records'?String(amount):money(amount);
      const caption=document.createElement('small');caption.textContent=note;
      const heading=document.createElement('div');heading.className='finance-kpi-heading';heading.append(title);
      if(label!=='Records'){
        const toggle=document.createElement('button');toggle.type='button';toggle.className='finance-amount-toggle';
        const shown=svgNode('svg',{viewBox:'0 0 24 24',width:16,height:16,fill:'none',stroke:'currentColor','stroke-width':1.6,'aria-hidden':'true',focusable:'false'});
        shown.append(svgNode('path',{d:'M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12Z'}),svgNode('circle',{cx:12,cy:12,r:3}));
        const slash=svgNode('path',{d:'M3 3 21 21'});shown.append(slash);toggle.append(shown);
        const update=()=>{
          const hidden=hiddenAmounts.has(label);
          value.textContent=hidden?'••••••':money(amount);
          value.setAttribute('aria-label',hidden?'Amount hidden':money(amount));
          toggle.setAttribute('aria-label',`${hidden?'Show':'Hide'} ${label.toLowerCase()} amount`);
          toggle.setAttribute('aria-pressed',String(hidden));
          toggle.title=`${hidden?'Show':'Hide'} amount`;slash.style.display=hidden?'':'none';
        };
        toggle.addEventListener('click',()=>{
          if(hiddenAmounts.has(label))hiddenAmounts.delete(label);else hiddenAmounts.add(label);
          try { sessionStorage.setItem('finance.hiddenAmounts',JSON.stringify([...hiddenAmounts])); } catch (_) {}
          update();
        });update();heading.append(toggle);
      }
      item.append(heading,value,caption);kpis.append(item);
    });
    chart('financeFlowChart');chart('financeIncomeChart',true);breakdown();
    const table=el('analysisDataBody');table.replaceChildren();report.monthly.forEach(row=>{const tr=document.createElement('tr');[row.period,money(row.income),money(row.outgoing)].forEach(value=>{const td=document.createElement('td');td.textContent=value;tr.append(td);});table.append(tr);});
    el('analysisMessage').textContent=`${report.basis==='all'?'All records, including pending.':'Received / paid records only.'} ${report.created_date_count} use creation date. ${report.suggested_count} expense records use suggested category mappings; ${report.unclassified_count} need classification. ${report.review_count} records need allocation review. ${report.omitted_count} invalid-date/amount records omitted.`;
    el('analysisResults').hidden=false;
  };
  const loadAnalysis = async () => {
    if(el('financeAnalysisView').hidden)return;
    if(request)request.abort();request=new AbortController();
    el('analysisResults').hidden=true;el('analysisMessage').textContent='Loading analysis…';
    const params=new URLSearchParams({bank_id:el('bankFilter').value||'ALL',currency:el('analysisCurrency').value,basis:el('analysisBasis').value,start:el('analysisStart').value,end:el('analysisEnd').value});
    try{const response=await fetch('/api/finance/analysis?'+params,{signal:request.signal});const out=await response.json();if(!response.ok)throw new Error(out.error||'Analysis unavailable');report=out.data;render();}
    catch(error){if(error.name!=='AbortError')el('analysisMessage').textContent=error.message;}
  };
  const tab = name => {
    ['Records','Analysis'].forEach(item=>{const active=item===name;el('finance'+item+'Tab').classList.toggle('active',active);el('finance'+item+'Tab').setAttribute('aria-selected',String(active));el('finance'+item+'Tab').tabIndex=active?0:-1;el('finance'+item+'View').hidden=!active;});
    if(name==='Analysis')loadAnalysis();
  };
  window.financeView={
    populateCategory:populate,categoryValue,refresh:loadAnalysis,
    async init(){
      const response=await fetch('/api/finance/categories');if(!response.ok)throw new Error('Unable to load category choices. Refresh to try again.');metadata=await response.json();
      metadata.income=Object.assign(Object.create(null),metadata.income);metadata.expense=Object.assign(Object.create(null),metadata.expense);
      // Saved custom choices are shared naturally through existing record values.
      (metadata.structured||[]).forEach(row=>{if(!row.category)return;const [group,...rest]=row.category.split(' / '),child=rest.join(' / ');const defs=row.type==='INCOME'?metadata.income:metadata.expense;defs[group]??=[];if(child&&!defs[group].includes(child))defs[group].push(child);});
      const currency=el('analysisCurrency');currency.replaceChildren();[...new Set(['LKR',...metadata.currencies])].forEach(name=>option(currency,name,name));populate();
    }
  };
  el('categoryGroup').addEventListener('change',()=>{el('legacyCategoryNote').hidden=true;children();});el('categoryChild').addEventListener('change',custom);
  el('type').addEventListener('change',()=>{populate();el('status').value=el('type').value==='INCOME'?'RECEIVED':'PAID';});
  ['Records','Analysis'].forEach(name=>{const button=el('finance'+name+'Tab');button.addEventListener('click',()=>tab(name));button.addEventListener('keydown',event=>{if(['ArrowRight','ArrowLeft','Home','End'].includes(event.key)){event.preventDefault();const target=event.key==='Home'?'Records':event.key==='End'?'Analysis':name==='Records'?'Analysis':'Records';tab(target);el('finance'+target+'Tab').focus();}});});
  el('applyAnalysis').addEventListener('click',loadAnalysis);el('expenseBack').addEventListener('click',()=>breakdown());
})();
