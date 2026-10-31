// static/js/cash_advances.js
(() => {
  const $ = (id) => document.getElementById(id);

  let me = null;
  let selectedAdvanceId = null;
  let records = new Map();
  let editingRecord = null;
  let editingExpenseId = null;
  let savingRecord = false;
  let savingExpense = false;
  let savingTopup = false;

  const canEdit = () => me?.role === "ADMIN" || Number(me?.permissions?.CASH_ADVANCES?.can_edit) === 1;
  const isClosed = record => Number(record?.closed || 0) === 1 || record?.status === "CLOSED";
  const currentRecord = () => records.get(selectedAdvanceId);
  const today = () => {
    const parts = new Intl.DateTimeFormat('en-CA', {timeZone: 'Asia/Colombo', year: 'numeric', month: '2-digit', day: '2-digit'}).formatToParts(new Date());
    const part = key => parts.find(item => item.type === key).value;
    return `${part('year')}-${part('month')}-${part('day')}`;
  };

  function openDialog(id) {
    const dialog = $(id);
    dialog.querySelectorAll('.cash-dialog-record').forEach(label => {
      const record = currentRecord();
      label.textContent = record ? `Record #${record.id} · ${record.employee_username} · ${record.currency}` : '';
    });
    if (!dialog.open) dialog.showModal();
  }

  function setCashView(view) {
    ['Records', 'Summary'].forEach(name => {
      const active = name.toLowerCase() === view;
      $(`cash${name}Tab`).classList.toggle('is-active', active);
      $(`cash${name}Tab`).setAttribute('aria-selected', String(active));
      $(`cash${name}Tab`).tabIndex = active ? 0 : -1;
      $(`cash${name}Panel`).hidden = !active;
    });
    $('cashSelectedPanel').hidden = view !== 'records' || !currentRecord();
  }

  function updateSelectedRecord() {
    const record = currentRecord();
    $('cashSelectedPanel').hidden = !record || $('cashRecordsPanel').hidden;
    document.querySelectorAll('[data-cash-row]').forEach(row => {
      row.classList.toggle('is-selected', Number(row.dataset.cashRow) === selectedAdvanceId);
    });
    if (!record) return;
    $('cashSelectedTitle').textContent = `Record #${record.id} · ${record.employee_username}`;
    $('cashSelectedPurpose').textContent = `${record.purpose || 'No purpose specified'} · ${isClosed(record) ? 'Closed' : 'Open'}`;
    const totals = $('cashSelectedTotals');
    totals.replaceChildren();
    [['Given', record.amount_given], ['Top-ups', record.topups_total], ['Spent', record.spent_total], ['Balance', record.balance]].forEach(([label, value]) => {
      const item = document.createElement('div');
      const caption = document.createElement('span'); caption.className = 'cash-total-label'; caption.textContent = label;
      const amount = document.createElement('strong'); amount.className = 'cash-total-value';
      const currency = document.createElement('small'); currency.textContent = record.currency;
      amount.append(currency, new Intl.NumberFormat('en-GB', {minimumFractionDigits: 2, maximumFractionDigits: 2}).format(Number(value || 0)));
      item.append(caption, amount); totals.append(item);
    });
    $('openExpenseBtn').hidden = !canEdit() || isClosed(record);
    if ($('openTopupBtn')) $('openTopupBtn').hidden = me?.role !== 'ADMIN' || isClosed(record);
    if ($('closeAdvanceBtn')) { $('closeAdvanceBtn').hidden = isClosed(record); $('closeAdvanceBtn').disabled = !canEdit(); }
    if ($('reopenAdvanceBtn')) { $('reopenAdvanceBtn').hidden = !isClosed(record); $('reopenAdvanceBtn').disabled = !canEdit(); }
  }

  function setSelectValue(id, value, label) {
    const select = $(id);
    select.querySelectorAll('[data-legacy-option]').forEach(option => option.remove());
    if (![...select.options].some(option => option.value === String(value))) {
      const option = new Option(label || String(value), value);
      option.dataset.legacyOption = 'true'; select.add(option);
    }
    select.value = value;
  }

  function openRecordForm(record = null) {
    if (me?.role !== 'ADMIN') return;
    if (record && isClosed(record)) return showMsg('selMsg', 'Reopen this record before editing it.', false);
    editingRecord = record ? {...record} : null;
    $('cashRecordForm').reset();
    $('advEmployee').value = me.user;
    if (record) {
      setSelectValue('advEmployee', record.employee_username, record.employee_username);
      setSelectValue('advBank', record.bank_id ?? '', record.bank_name || 'No bank assigned');
      setSelectValue('advCurrency', record.currency, record.currency);
      $('advAmount').value = record.amount_given;
      $('advPurpose').value = record.purpose || '';
      $('advGivenDate').value = record.given_date || '';
      $('advProofLink').value = record.proof_link || '';
    } else {
      ['advEmployee', 'advBank', 'advCurrency'].forEach(id => $(id).querySelectorAll('[data-legacy-option]').forEach(option => option.remove()));
      if ([...$('advEmployee').options].some(option => option.value === me.user)) $('advEmployee').value = me.user;
      if (!$('advBank').value && $('advBank').options.length) $('advBank').selectedIndex = 0;
      $('advGivenDate').value = today();
    }
    const currencyLocked = !!record && (Number(record.topups_total) > 0 || Number(record.spent_total) > 0);
    $('advCurrency').disabled = currencyLocked;
    $('cashRecordTitle').textContent = record ? `Edit petty cash #${record.id}` : 'New petty cash';
    $('cashRecordHint').textContent = currencyLocked ? 'Currency is fixed because this record already has expenses or top-ups.' : 'Enter the initial allocation details.';
    $('createAdvanceBtn').textContent = record ? 'Save changes' : 'Create petty cash';
    showMsg('advMsg', '');
    openDialog('cashRecordDialog');
  }

  function openExpenseForm(expense = null) {
    if (!currentRecord() || !canEdit() || isClosed(currentRecord())) return;
    editingExpenseId = expense?.id || null;
    $('cashExpenseForm').reset();
    $('expCategory').value = expense?.category || '';
    $('expDescription').value = expense?.description || '';
    $('expPaidBy').value = expense?.paid_by || 'COMPANY_ADVANCE';
    $('expAmount').value = expense?.amount || '';
    $('expDate').value = expense?.spent_date || today();
    $('expProof').value = expense?.proof_link || '';
    $('cashExpenseTitle').textContent = expense ? `Edit expense #${expense.id}` : 'Add expense';
    $('addExpenseBtn').textContent = expense ? 'Save changes' : 'Add expense';
    showMsg('expMsg', '');
    openDialog('cashExpenseDialog');
  }

  function safeText(v){
    return String(v ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'}[char]));
  }

  function normalizeUrl(u){
    if(String(u || "").startsWith("/api/storage/files/"))return u;
    const s = (u || "").trim();
    if(!s) return "";
    const low = s.toLowerCase();
    if(low.startsWith("http://") || low.startsWith("https://") || low.startsWith("mailto:") || low.startsWith("tel:")) return s;
    return "https://" + s;
  }

  async function safeJson(res){
    const ct = (res.headers.get("content-type") || "").toLowerCase();
    const txt = await res.text();
    if(!ct.includes("application/json")){
      throw new Error("Non-JSON response (maybe login/redirect): " + txt.slice(0,120));
    }
    return JSON.parse(txt);
  }

  function showMsg(id, text, ok=true){
    const el = $(id);
    if(!el) return;
    el.textContent = text || "";
    el.className = "msg " + (ok ? "ok" : "bad");
  }

  async function apiGet(url){
    const res = await fetch(url);
    const data = await safeJson(res);
    if(!res.ok) throw new Error(data?.error || "Request failed");
    return data;
  }

  async function apiPost(url, bodyObj=null){
    const res = await fetch(url, {
      method:"POST",
      headers: bodyObj ? {"Content-Type":"application/json"} : undefined,
      body: bodyObj ? JSON.stringify(bodyObj) : undefined
    });
    const data = await safeJson(res);
    if(!res.ok) throw new Error(data?.error || "Request failed");
    return data;
  }

  async function apiPut(url, bodyObj){
    const res = await fetch(url, {
      method:"PUT",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify(bodyObj || {})
    });
    const data = await safeJson(res);
    if(!res.ok) throw new Error(data?.error || "Request failed");
    return data;
  }

  async function apiDel(url){
    const res = await fetch(url, {method:"DELETE"});
    const data = await safeJson(res);
    if(!res.ok) throw new Error(data?.error || "Request failed");
    return data;
  }

  async function loadMe(){
    me = document.body.dataset.appRole !== 'ADMIN' && window.erpWorkspaceMe
      ? await window.erpWorkspaceMe : await apiGet('/api/me');

    if($("closeAdvanceBtn")) $("closeAdvanceBtn").disabled = (me?.role !== "ADMIN");
    if($("reopenAdvanceBtn")) $("reopenAdvanceBtn").disabled = (me?.role !== "ADMIN");
    if($("addTopupBtn")) $("addTopupBtn").disabled = (me?.role !== "ADMIN");

    return me;
  }

  // -------------------------
  // Dropdown helpers
  // -------------------------
  async function loadEmployees(){
    const selA = $("advEmployee");
    const selS = $("sumEmployee");
  
    const out = await apiGet("/api/users/employees");
    const rows = out.data || [];
  
    // ADMIN
    if(me?.role === "ADMIN"){
      if(selA){
        selA.innerHTML = "";
        const all = document.createElement("option");
        all.value = "";
        all.textContent = "Select employee";
        selA.appendChild(all);
  
        rows.forEach(u=>{
          const opt = document.createElement("option");
          opt.value = u.username;
          opt.textContent = `${u.username} (${u.role})`;
          selA.appendChild(opt);
        });
      }
  
      if(selS){
        selS.innerHTML = "";
        const all = document.createElement("option");
        all.value = "ALL";
        all.textContent = "ALL";
        selS.appendChild(all);
  
        rows.forEach(u=>{
          const opt = document.createElement("option");
          opt.value = u.username;
          opt.textContent = `${u.username} (${u.role})`;
          selS.appendChild(opt);
        });
      }
  
      if(me?.user){
        const found = rows.find(x => x.username === me.user);
        if(found && selA) selA.value = me.user;
        if(found && selS) selS.value = me.user;
      }
  
      return;
    }
  
    // EMP
    if(selA){
      selA.innerHTML = "";
      const opt = document.createElement("option");
      opt.value = me?.user || "";
      opt.textContent = me?.user || "My Advance";
      selA.appendChild(opt);
      selA.value = me?.user || "";
      selA.disabled = true;
    }
  
    if(selS){
      selS.innerHTML = "";
      const opt = document.createElement("option");
      opt.value = me?.user || "";
      opt.textContent = me?.user || "My Summary";
      selS.appendChild(opt);
      selS.value = me?.user || "";
      selS.disabled = true;
    }
  }

  async function loadBanks(){
    const sel = $("advBank");
    if(!sel) return;
    const out = await apiGet("/api/banks");
    const rows = out || [];
    sel.innerHTML = "";
    rows.forEach(b=>{
      const opt = document.createElement("option");
      opt.value = b.id;
      opt.textContent = b.name;
      sel.appendChild(opt);
    });
  }

  // -------------------------
  // ADVANCES
  // -------------------------
  function renderAdvances(rows){
    const body = $("advTableBody");
    if(!body) return;
    body.innerHTML = "";

    if(!rows || rows.length === 0){
      body.innerHTML = '<tr><td colspan="13" class="cash-empty-row">No petty cash records found.</td></tr>';
      return;
    }

    rows.forEach(r=>{
      const tr = document.createElement("tr");
      tr.style.cursor = "pointer";
      tr.dataset.cashRow = String(r.id);

      const proofBtn = r.proof_link
        ? `<a class="btn mini ghost" target="_blank" rel="noopener" href="${safeText(normalizeUrl(r._storage_links?.proof_link || r.proof_link))}">Open</a>`
        : `<span class="muted">—</span>`;

      const bal = Number(r.balance || 0);
      const balPill = bal < 0
        ? `<span class="pill neg">${bal.toFixed(2)}</span>`
        : `<span class="pill pos">${bal.toFixed(2)}</span>`;

      const status = r.status || (Number(r.closed||0) === 1 ? "CLOSED" : "OPEN");

      const actionHtml = (me?.role === "ADMIN")
        ? `
          <button class="btn mini ghost" data-adv-sel="${r.id}">Select</button>
          ${!isClosed(r) ? `<button class="btn mini" data-adv-edit="${r.id}">Edit</button>` : ``}
          <button class="btn mini danger" data-adv-del="${r.id}">Delete</button>
        `
        : `<button class="btn mini ghost" data-adv-sel="${r.id}">Select</button>`;

      tr.innerHTML = `
        <td><b>${r.id}</b></td>
        <td>${safeText(r.employee_username)}</td>
        <td>${safeText(r.bank_name || "")}</td>
        <td>${safeText(r.currency)}</td>
        <td><b>${Number(r.amount_given||0).toFixed(2)}</b></td>
        <td><b>${Number(r.topups_total||0).toFixed(2)}</b></td>
        <td><b>${Number(r.spent_total||0).toFixed(2)}</b></td>
        <td>${balPill}</td>
        <td><span class="pill">${status}</span></td>
        <td>${safeText(r.purpose)}</td>
        <td class="mono">${safeText(r.given_date)}</td>
        <td>${proofBtn}</td>
        <td><div class="cash-actions">${actionHtml}</div></td>
      `;

      tr.addEventListener("click", event => {
        if (!event.target.closest('a, button')) selectAdvance(r.id).catch(err => showMsg('selMsg', err.message, false));
      });
      tr.querySelector(`[data-adv-edit="${r.id}"]`)?.addEventListener('click', event => {
        event.stopPropagation(); openRecordForm(r);
      });

      const selBtn = tr.querySelector(`[data-adv-sel="${r.id}"]`);
      if(selBtn){
        selBtn.addEventListener("click", (e)=>{
          e.stopPropagation();
          selectAdvance(r.id).catch(err => showMsg('selMsg', err.message, false));
        });
      }

      const delBtn = tr.querySelector(`[data-adv-del="${r.id}"]`);
      if(delBtn){
        delBtn.addEventListener("click", async (e)=>{
          e.stopPropagation();
          if(!confirm(`Delete advance ${r.id}? This will remove its expenses/topups too.`)) return;
          try{
            await apiDel(`/api/advances/${r.id}`);
            if(selectedAdvanceId === Number(r.id)){
              selectedAdvanceId = null;
              if($("expTableBody")) $("expTableBody").innerHTML = "";
              if($("topupTableBody")) $("topupTableBody").innerHTML = "";
            }
            showMsg("selMsg", `Advance ${r.id} deleted`, true);
            await refreshAll();
          }catch(err){
            showMsg("selMsg", err.message, false);
          }
        });
      }

      body.appendChild(tr);
    });
  }

  async function loadAdvances(){
    const out = await apiGet("/api/advances");
    records = new Map((out.data || []).map(record => [Number(record.id), record]));
    if (selectedAdvanceId && !records.has(selectedAdvanceId)) selectedAdvanceId = null;
    renderAdvances(out.data || []);
    updateSelectedRecord();
  }

  async function selectAdvance(id){
    selectedAdvanceId = Number(id);
    setCashView('records');
    updateSelectedRecord();
    showMsg("selMsg", `Selected petty cash #${selectedAdvanceId}`, true);
    await Promise.all([loadTopupsForSelected(), loadExpensesForSelected()]);
    $('cashSelectedPanel').scrollIntoView({block: 'start', behavior: 'auto'});
    $('cashSelectedTitle').focus({preventScroll: true});
  }

  // -------------------------
  // EXPENSES
  // -------------------------
  function renderExpenses(rows){
    const body = $("expTableBody");
    if(!body) return;
    body.innerHTML = "";

    if(!rows || rows.length === 0){
      body.innerHTML = '<tr><td colspan="10" class="cash-empty-row">No expenses added to this record yet.</td></tr>';
      return;
    }

    rows.forEach(r=>{
      const tr = document.createElement("tr");

      const proofBtn = r.proof_link
        ? `<a class="btn mini ghost" target="_blank" rel="noopener" href="${safeText(normalizeUrl(r._storage_links?.proof_link || r.proof_link))}">Open</a>`
        : `<span class="muted">—</span>`;

      const paidBy = (r.paid_by || "COMPANY_ADVANCE").toUpperCase();
      const paidByLabel = (paidBy === "EMPLOYEE_PERSONAL")
        ? `<span class="pill neg">PERSONAL</span>`
        : `<span class="pill">COMPANY</span>`;

      const actionHtml = canEdit() && !isClosed(currentRecord()) ? `
        <button class="btn mini ghost" data-exp-edit="${r.id}">Edit</button>
        <button class="btn mini danger" data-exp-del="${r.id}">Delete</button>
      ` : '<span class="muted">—</span>';

      tr.innerHTML = `
        <td><b>${r.id}</b></td>
        <td>${safeText(r.category)}</td>
        <td>${safeText(r.description)}</td>
        <td>${paidByLabel}</td>
        <td><b>${Number(r.amount||0).toFixed(2)}</b></td>
        <td>${safeText(r.spent_date)}</td>
        <td>${proofBtn}</td>
        <td>${safeText(r.created_by)}</td>
        <td>${safeText(r.created_at)}</td>
        <td>${actionHtml}</td>
      `;

      tr.querySelector(`[data-exp-edit="${r.id}"]`)?.addEventListener('click', event => {
        event.stopPropagation(); openExpenseForm(r);
      });

      tr.querySelector(`[data-exp-del="${r.id}"]`)?.addEventListener("click", async (e)=>{
        e.stopPropagation();
        if(!confirm(`Delete expense ${r.id}?`)) return;
        try{
          await apiDel(`/api/expenses/${r.id}`);
          showMsg("expMsg", `Expense ${r.id} deleted`, true);
          await refreshAll();
        }catch(err){
          showMsg("expMsg", err.message, false);
        }
      });

      body.appendChild(tr);
    });
  }

  async function loadExpensesForSelected(){
    const body = $("expTableBody");
    if(!body) return;

    if(!selectedAdvanceId){
      body.innerHTML = "";
      showMsg("expMsg", "Select an advance to view expenses.", true);
      return;
    }

    const id = selectedAdvanceId;
    const out = await apiGet(`/api/advances/${id}/expenses`);
    if (id === selectedAdvanceId) renderExpenses(out.data || []);
  }

  async function addExpense(){
    if (savingExpense) return;
    savingExpense = true; $('addExpenseBtn').disabled = true;
    try {
      if (!selectedAdvanceId) throw new Error('Select a petty cash record first.');
      const payload = {
        category: $('expCategory').value.trim(), description: $('expDescription').value.trim(),
        paid_by: $('expPaidBy').value, amount: $('expAmount').value,
        spent_date: $('expDate').value, proof_link: normalizeUrl($('expProof').value)
      };
      if (editingExpenseId) await apiPut(`/api/expenses/${editingExpenseId}`, payload);
      else await apiPost(`/api/advances/${selectedAdvanceId}/expenses`, payload);
      $('cashExpenseDialog').close();
      showMsg('selMsg', editingExpenseId ? 'Expense updated.' : 'Expense added.');
      await refreshAll();
    } catch (err) { showMsg($('cashExpenseDialog').open ? 'expMsg' : 'selMsg', $('cashExpenseDialog').open ? err.message : `Expense saved, but the view could not refresh. ${err.message}`, false); }
    finally { savingExpense = false; $('addExpenseBtn').disabled = false; }
  }

  // -------------------------
  // TOPUPS
  // -------------------------
  function renderTopups(rows){
    const body = $("topupTableBody");
    if(!body) return;
    body.innerHTML = "";

    if(!rows || rows.length === 0) {
      body.innerHTML = '<tr><td colspan="8" class="cash-empty-row">No top-ups added to this record yet.</td></tr>';
      return;
    }

    rows.forEach(t=>{
      const tr = document.createElement("tr");

      const proofBtn = t.proof_link
        ? `<a class="btn mini ghost" target="_blank" rel="noopener" href="${safeText(normalizeUrl(t._storage_links?.proof_link || t.proof_link))}">Open</a>`
        : `<span class="muted">—</span>`;

      const ref = (t.ref_type && t.ref_id)
        ? `<span class="pill">${safeText(t.ref_type)} #${safeText(t.ref_id)}</span>`
        : `<span class="muted">—</span>`;

      const actionHtml = (me?.role === "ADMIN")
        ? `<button class="btn mini danger" data-top-del="${t.id}">Delete</button>`
        : `<span class="muted">—</span>`;

      tr.innerHTML = `
        <td><b>${t.id}</b></td>
        <td><b>${Number(t.amount||0).toFixed(2)}</b></td>
        <td class="mono">${safeText(t.topup_date)}</td>
        <td>${proofBtn}</td>
        <td>${ref}</td>
        <td>${safeText(t.created_by)}</td>
        <td class="mono">${safeText(t.created_at)}</td>
        <td>${actionHtml}</td>
      `;

      const del = tr.querySelector(`[data-top-del="${t.id}"]`);
      if(del){
        del.addEventListener("click", async (e)=>{
          e.stopPropagation();
          if(!confirm(`Delete top-up ${t.id}?`)) return;
          try{
            await apiDel(`/api/topups/${t.id}`);
            showMsg("topupMsg", "Top-up deleted.", true);
            await refreshAll();
          }catch(err){
            showMsg("topupMsg", err.message, false);
          }
        });
      }

      body.appendChild(tr);
    });
  }

  async function loadTopupsForSelected(){
    const body = $("topupTableBody");
    if(!body) return;

    if(!selectedAdvanceId){
      body.innerHTML = "";
      return;
    }

    const id = selectedAdvanceId;
    const out = await apiGet(`/api/advances/${id}/topups`);
    if (id === selectedAdvanceId) renderTopups(out.data || []);
  }

  async function addTopup(){
    if (savingTopup) return;
    savingTopup = true; $('addTopupBtn').disabled = true;
    try{
      if(me?.role !== "ADMIN") throw new Error("Admin only.");
      if(!selectedAdvanceId) throw new Error("Select an advance first.");

      const amount = $("topupAmount")?.value || "";
      const topup_date = $("topupDate")?.value || "";
      const proof_link = normalizeUrl($("topupProof")?.value || "");

      const ref_type = ($("topupRefType")?.value || "").trim().toUpperCase();
      const ref_id = ($("topupRefId")?.value || "").trim();
      const note = ($("topupNote")?.value || "").trim();

      const amt = Number(amount);
      if(!isFinite(amt) || amt <= 0) throw new Error("Top-up amount must be > 0.");

      await apiPost(`/api/advances/${selectedAdvanceId}/topups`, {
        amount, topup_date, proof_link, ref_type, ref_id, note
      });

      $('cashTopupDialog').close();
      showMsg("selMsg", "Top-up added.", true);

      if($("topupAmount")) $("topupAmount").value = "";
      if($("topupProof")) $("topupProof").value = "";
      if($("topupRefId")) $("topupRefId").value = "";
      if($("topupNote")) $("topupNote").value = "";

      await refreshAll();
    }catch(err){
      showMsg($('cashTopupDialog').open ? 'topupMsg' : 'selMsg', $('cashTopupDialog').open ? err.message : `Top-up saved, but the view could not refresh. ${err.message}`, false);
    } finally { savingTopup = false; $('addTopupBtn').disabled = false; }
  }

  // -------------------------
  // CLOSE / REOPEN
  // -------------------------
  async function closeSelected(){
    try{
      if(me?.role !== "ADMIN") return showMsg("selMsg", "Admin only.", false);
      if(!selectedAdvanceId) return showMsg("selMsg", "Select an advance first.", false);

      await apiPost(`/api/advances/${selectedAdvanceId}/close`);
      showMsg("selMsg", `Advance ${selectedAdvanceId} closed`, true);
      await refreshAll();
    }catch(err){
      showMsg("selMsg", err.message, false);
    }
  }

  async function reopenSelected(){
    try{
      if(me?.role !== "ADMIN") return showMsg("selMsg", "Admin only.", false);
      if(!selectedAdvanceId) return showMsg("selMsg", "Select an advance first.", false);

      await apiPost(`/api/advances/${selectedAdvanceId}/reopen`);
      showMsg("selMsg", `Advance ${selectedAdvanceId} reopened`, true);
      await refreshAll();
    }catch(err){
      showMsg("selMsg", err.message, false);
    }
  }

  // -------------------------
  // CREATE ADVANCE (ADMIN)
  // -------------------------
  async function createAdvance(){
    if (savingRecord) return;
    savingRecord = true; $('createAdvanceBtn').disabled = true;
    try {
      if (me?.role !== 'ADMIN') throw new Error('Admin only.');
      const payload = {
        employee_username: $('advEmployee').value, bank_id: $('advBank').value,
        currency: $('advCurrency').value, amount_given: $('advAmount').value,
        purpose: $('advPurpose').value.trim(), given_date: $('advGivenDate').value,
        proof_link: normalizeUrl($('advProofLink').value)
      };
      if (!payload.employee_username || payload.employee_username === 'ALL') throw new Error('Select an employee.');
      if (!Number.isFinite(Number(payload.amount_given)) || Number(payload.amount_given) <= 0) throw new Error('Amount must be greater than zero.');
      let id;
      if (editingRecord) {
        payload.edit_version = editingRecord.edit_version;
        await apiPut(`/api/advances/${editingRecord.id}`, payload);
        id = editingRecord.id;
      } else {
        const out = await apiPost('/api/advances', payload);
        id = out.data.id;
      }
      $('cashRecordDialog').close();
      await loadAdvances();
      await selectAdvance(id);
      showMsg('selMsg', editingRecord ? `Petty cash #${id} updated.` : `Petty cash #${id} created.`);
    } catch (err) { showMsg($('cashRecordDialog').open ? 'advMsg' : 'selMsg', $('cashRecordDialog').open ? err.message : `Record saved, but the view could not refresh. ${err.message}`, false); }
    finally { savingRecord = false; $('createAdvanceBtn').disabled = false; }
  }

  // -------------------------
  // SUMMARY
  // -------------------------
  async function loadSummary(){
    try{
      const emp = $("sumEmployee")?.value || "ALL";
      const month = ($("sumMonth")?.value || "").trim();
      const status = $("sumStatus")?.value || "ALL";

      const params = new URLSearchParams();
      if(emp && emp !== "ALL") params.set("employee", emp);
      if(month) params.set("month", month);
      if(status && status !== "ALL") params.set("status", status);

      const out = await apiGet("/api/advances/summary?" + params.toString());
      const rows = out.data || [];

      const body = $("sumBody");
      body.innerHTML = "";
      rows.forEach(r=>{
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${safeText(r.employee_username)}</td>
          <td>${safeText(r.currency)}</td>
          <td><b>${Number(r.total_given||0).toFixed(2)}</b></td>
          <td><b>${Number(r.total_topups||0).toFixed(2)}</b></td>
          <td><b>${Number(r.total_spent||0).toFixed(2)}</b></td>
          <td><b>${Number(r.total_balance||0).toFixed(2)}</b></td>
          <td class="muted">${safeText(r.advances_count||0)}</td>
        `;
        body.appendChild(tr);
      });

      showMsg("sumMsg", `Loaded ${rows.length} row(s).`, true);
    }catch(err){
      showMsg("sumMsg", err.message, false);
    }
  }

  function exportSummaryCsv(){
    const emp = $("sumEmployee")?.value || "ALL";
    const month = ($("sumMonth")?.value || "").trim();
    const status = $("sumStatus")?.value || "ALL";

    const params = new URLSearchParams();
    if(emp && emp !== "ALL") params.set("employee", emp);
    if(month) params.set("month", month);
    if(status && status !== "ALL") params.set("status", status);

    window.location.href = "/api/advances/summary.csv?" + params.toString();
  }

  async function refreshAll(){
    await loadAdvances();
    await Promise.all([loadTopupsForSelected(), loadExpensesForSelected()]);
    if (!$('cashSummaryPanel').hidden) await loadSummary();
  }

  // -------------------------
  // INIT
  // -------------------------
  async function init(){
    await loadMe();

    await Promise.all([loadEmployees(), me?.role === 'ADMIN' ? loadBanks() : Promise.resolve()]);

    $('cashRecordForm')?.addEventListener('submit', event => { event.preventDefault(); createAdvance(); });
    $('cashExpenseForm').addEventListener('submit', event => { event.preventDefault(); addExpense(); });
    $('cashTopupForm')?.addEventListener('submit', event => { event.preventDefault(); addTopup(); });
    $('newAdvanceBtn')?.addEventListener('click', () => openRecordForm());
    $('openExpenseBtn').addEventListener('click', () => openExpenseForm());
    $('openTopupBtn')?.addEventListener('click', () => {
      $('cashTopupForm').reset(); $('topupDate').value = today(); showMsg('topupMsg', ''); openDialog('cashTopupDialog');
    });
    document.querySelectorAll('[data-close-dialog]').forEach(button => {
      button.addEventListener('click', () => button.closest('dialog').close());
    });
    document.querySelectorAll('.cash-dialog').forEach(dialog => {
      const busy = () => savingRecord || savingExpense || savingTopup;
      dialog.addEventListener('cancel', event => { if (busy()) event.preventDefault(); });
      dialog.querySelectorAll('[data-close-dialog]').forEach(button => button.addEventListener('click', event => {
        if (busy()) { event.preventDefault(); event.stopImmediatePropagation(); }
      }, true));
    });
    ['Records', 'Summary'].forEach(name => {
      const tab = $(`cash${name}Tab`);
      tab.addEventListener('click', () => { setCashView(name.toLowerCase()); if (name === 'Summary') loadSummary(); });
      tab.addEventListener('keydown', event => {
        if (['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) {
          event.preventDefault();
          const target = event.key === 'Home' ? $('cashRecordsTab') : event.key === 'End' ? $('cashSummaryTab') : name === 'Records' ? $('cashSummaryTab') : $('cashRecordsTab');
          target.click(); target.focus();
        }
      });
    });

    $("closeAdvanceBtn")?.addEventListener("click", closeSelected);
    $("reopenAdvanceBtn")?.addEventListener("click", reopenSelected);

    $("refreshAdvBtn")?.addEventListener("click", async () => {
      $('refreshAdvBtn').disabled = true;
      try { await refreshAll(); showMsg('selMsg', 'Records refreshed.'); }
      catch (err) { showMsg('selMsg', err.message, false); }
      finally { $('refreshAdvBtn').disabled = false; }
    });
    $("loadSummaryBtn")?.addEventListener("click", loadSummary);
    $("exportSummaryBtn")?.addEventListener("click", exportSummaryCsv);

    await refreshAll();
    if ($('newAdvanceBtn')) $('newAdvanceBtn').disabled = false;
  }

  init().catch(err => {
    console.error(err);
    showMsg("selMsg", "Init error: " + err.message, false);
  });
})();