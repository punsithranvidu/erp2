// static/js/cash_advances.js
(() => {
  const $ = (id) => document.getElementById(id);

  let me = null;
  let selectedAdvanceId = null;

  function safeText(v){ return (v===null||v===undefined) ? "" : String(v); }

  function normalizeUrl(u){
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
    me = await apiGet("/api/me");

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
        all.value = "ALL";
        all.textContent = "ALL";
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
      showMsg("selMsg", "No advances found.", true);
      return;
    }

    rows.forEach(r=>{
      const tr = document.createElement("tr");
      tr.style.cursor = "pointer";

      const proofBtn = r.proof_link
        ? `<a class="btn mini ghost" target="_blank" rel="noopener" href="${normalizeUrl(r.proof_link)}">Open</a>`
        : `<span class="muted">—</span>`;

      const bal = Number(r.balance || 0);
      const balPill = bal < 0
        ? `<span class="pill neg">${bal.toFixed(2)}</span>`
        : `<span class="pill pos">${bal.toFixed(2)}</span>`;

      const status = r.status || (Number(r.closed||0) === 1 ? "CLOSED" : "OPEN");

      const actionHtml = (me?.role === "ADMIN")
        ? `
          <button class="btn mini ghost" data-adv-sel="${r.id}">Select</button>
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
        <td>${actionHtml}</td>
      `;

      tr.addEventListener("click", () => selectAdvance(r.id));

      const selBtn = tr.querySelector(`[data-adv-sel="${r.id}"]`);
      if(selBtn){
        selBtn.addEventListener("click", (e)=>{
          e.stopPropagation();
          selectAdvance(r.id);
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
    renderAdvances(out.data || []);
  }

  async function selectAdvance(id){
    selectedAdvanceId = Number(id);
    showMsg("selMsg", `Selected Advance ID: ${selectedAdvanceId}`, true);
    await loadTopupsForSelected();
    await loadExpensesForSelected();
  }

  // -------------------------
  // EXPENSES
  // -------------------------
  function renderExpenses(rows){
    const body = $("expTableBody");
    if(!body) return;
    body.innerHTML = "";

    if(!rows || rows.length === 0){
      showMsg("expMsg", "No expenses for this advance yet.", true);
      return;
    }

    rows.forEach(r=>{
      const tr = document.createElement("tr");

      const proofBtn = r.proof_link
        ? `<a class="btn mini ghost" target="_blank" rel="noopener" href="${normalizeUrl(r.proof_link)}">Open</a>`
        : `<span class="muted">—</span>`;

      const paidBy = (r.paid_by || "COMPANY_ADVANCE").toUpperCase();
      const paidByLabel = (paidBy === "EMPLOYEE_PERSONAL")
        ? `<span class="pill neg">PERSONAL</span>`
        : `<span class="pill">COMPANY</span>`;

      const actionHtml = `
        <button class="btn mini ghost" data-exp-edit="${r.id}">Edit</button>
        <button class="btn mini danger" data-exp-del="${r.id}">Delete</button>
      `;

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

      tr.querySelector(`[data-exp-edit="${r.id}"]`).addEventListener("click", async (e)=>{
        e.stopPropagation();
        try{
          const newCat = prompt("Category:", safeText(r.category)); if(newCat===null) return;
          const newDesc = prompt("Description:", safeText(r.description)); if(newDesc===null) return;

          const curPaid = (r.paid_by || "COMPANY_ADVANCE").toUpperCase();
          const newPaidBy = prompt("Paid By (COMPANY_ADVANCE or EMPLOYEE_PERSONAL):", curPaid);
          if(newPaidBy===null) return;

          const newAmt = prompt("Amount:", String(r.amount||"")); if(newAmt===null) return;
          const newDate = prompt("Spent Date (YYYY-MM-DD):", safeText(r.spent_date)); if(newDate===null) return;
          const newProof = prompt("Proof link:", safeText(r.proof_link)); if(newProof===null) return;

          await apiPut(`/api/expenses/${r.id}`, {
            category: newCat.trim(),
            description: newDesc.trim(),
            paid_by: newPaidBy.trim().toUpperCase(),
            amount: newAmt,
            spent_date: newDate.trim(),
            proof_link: newProof.trim()
          });

          showMsg("expMsg", `Expense ${r.id} updated`, true);
          await refreshAll();
        }catch(err){
          showMsg("expMsg", err.message, false);
        }
      });

      tr.querySelector(`[data-exp-del="${r.id}"]`).addEventListener("click", async (e)=>{
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

    const out = await apiGet(`/api/advances/${selectedAdvanceId}/expenses`);
    renderExpenses(out.data || []);
  }

  async function addExpense(){
    try{
      if(!selectedAdvanceId) throw new Error("Select an advance first.");

      const category = ($("expCategory")?.value || "").trim();
      const description = ($("expDescription")?.value || "").trim();
      const paid_by = ($("expPaidBy")?.value || "COMPANY_ADVANCE").trim();
      const amount = $("expAmount")?.value || "";
      const spent_date = $("expDate")?.value || "";
      const proof_link = normalizeUrl($("expProof")?.value || "");

      if(!category) throw new Error("Category is required.");
      const amt = Number(amount);
      if(!isFinite(amt) || amt <= 0) throw new Error("Amount must be > 0.");

      await apiPost(`/api/advances/${selectedAdvanceId}/expenses`, {
        category, description, paid_by, amount, spent_date, proof_link
      });

      showMsg("expMsg", "Expense added ✅", true);

      if($("expCategory")) $("expCategory").value = "";
      if($("expDescription")) $("expDescription").value = "";
      if($("expAmount")) $("expAmount").value = "";
      if($("expProof")) $("expProof").value = "";

      await refreshAll();
    }catch(err){
      showMsg("expMsg", err.message, false);
    }
  }

  // -------------------------
  // TOPUPS
  // -------------------------
  function renderTopups(rows){
    const body = $("topupTableBody");
    if(!body) return;
    body.innerHTML = "";

    if(!rows || rows.length === 0) return;

    rows.forEach(t=>{
      const tr = document.createElement("tr");

      const proofBtn = t.proof_link
        ? `<a class="btn mini ghost" target="_blank" rel="noopener" href="${normalizeUrl(t.proof_link)}">Open</a>`
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

    const out = await apiGet(`/api/advances/${selectedAdvanceId}/topups`);
    renderTopups(out.data || []);
  }

  async function addTopup(){
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

      showMsg("topupMsg", "Top-up added ✅", true);

      if($("topupAmount")) $("topupAmount").value = "";
      if($("topupProof")) $("topupProof").value = "";
      if($("topupRefId")) $("topupRefId").value = "";
      if($("topupNote")) $("topupNote").value = "";

      await refreshAll();
    }catch(err){
      showMsg("topupMsg", err.message, false);
    }
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
    try{
      if(me?.role !== "ADMIN") return showMsg("advMsg", "Admin only.", false);

      const employee_username = ($("advEmployee")?.value || "").trim();
      const bank_id = $("advBank")?.value || "";
      const currency = $("advCurrency")?.value || "LKR";
      const amount_given = $("advAmount")?.value || "";
      const purpose = ($("advPurpose")?.value || "").trim();
      const given_date = $("advGivenDate")?.value || "";
      const proof_link = normalizeUrl($("advProofLink")?.value || "");

      if(!employee_username || employee_username === "ALL") throw new Error("Select employee.");
      const amt = Number(amount_given);
      if(!isFinite(amt) || amt <= 0) throw new Error("Amount must be > 0.");

      const out = await apiPost("/api/advances", {
        employee_username, bank_id, currency, amount_given, purpose, given_date, proof_link
      });

      showMsg("advMsg", `Advance created (ID ${out.data?.id || "OK"})`, true);

      if($("advAmount")) $("advAmount").value = "";
      if($("advPurpose")) $("advPurpose").value = "";
      if($("advProofLink")) $("advProofLink").value = "";

      await loadAdvances();
    }catch(err){
      showMsg("advMsg", err.message, false);
    }
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
    await loadTopupsForSelected();
    await loadExpensesForSelected();
    // summary is optional (don’t force if you don’t want)
  }

  // -------------------------
  // INIT
  // -------------------------
  async function init(){
    await loadMe();

    if(me?.role === "ADMIN"){
      await loadBanks();
    }

    await loadEmployees();

    $("createAdvanceBtn")?.addEventListener("click", createAdvance);
    $("addExpenseBtn")?.addEventListener("click", addExpense);
    $("addTopupBtn")?.addEventListener("click", addTopup);

    $("closeAdvanceBtn")?.addEventListener("click", closeSelected);
    $("reopenAdvanceBtn")?.addEventListener("click", reopenSelected);

    $("refreshAdvBtn")?.addEventListener("click", refreshAll);
    $("loadSummaryBtn")?.addEventListener("click", loadSummary);
    $("exportSummaryBtn")?.addEventListener("click", exportSummaryCsv);

    await refreshAll();
    showMsg("expMsg", "Select an advance to view expenses.", true);
  }

  init().catch(err => {
    console.error(err);
    showMsg("selMsg", "Init error: " + err.message, false);
  });
})();