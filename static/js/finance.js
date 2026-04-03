/* =========================
   finance.js (FULL)
   Fix: Export + Summary should NOT force INCOME when Type = ALL
   ========================= */

   const $ = (id) => document.getElementById(id);

   let ME = null;
   let EDITING_ID = null;
   let FILTER_MONTH = "";
   let BANKS = [];      // active banks (for dropdown)
   let BANKS_ALL = [];  // admin list (active + disabled)
   let SELECTED_BANK_ID = "ALL"; // default = all banks
   
   function safeText(v){ return (v===null||v===undefined)?"":String(v); }
   
   function normalizeUrl(url){
     const u=(url||"").trim();
     if(!u) return "";
     if(/^https?:\/\//i.test(u)) return u;
     if(/^(mailto:|tel:)/i.test(u)) return u;
     return "https://" + u;
   }
   
   async function safeJson(res){
     const ct = (res.headers.get("content-type") || "").toLowerCase();
     if (ct.includes("application/json")) return await res.json();
     const text = await res.text();
     throw new Error("Server returned non-JSON (login/404). " + text.slice(0, 120));
   }
   
   function openLink(label, url){
     const fixed = normalizeUrl(url);
     if(!fixed) return `<span class="pill muted">${label}</span>`;
     return `<a class="pill link" href="${fixed}" target="_blank" rel="noopener">${label}</a>`;
   }
   
   function showMsg(id, text, ok=true){
     const el = $(id);
     if(!el) return;
     el.textContent = text;
     el.className = "msg " + (ok ? "ok" : "bad");
   }
   
   async function getMe(){
     const res = await fetch("/api/me");
     ME = await safeJson(res);
     $("createdBy").value = ME.user;
   
     const adminBox = $("adminBankAdd");
     if(adminBox){
       adminBox.style.display = (ME.role === "ADMIN") ? "block" : "none";
     }
   }
   
   /* ---------- BANKS ---------- */
   function setBankLabel(){
     if(SELECTED_BANK_ID === "ALL"){
       $("bankLabel").textContent = "ALL BANK ACCOUNTS";
       return;
     }
     const b = BANKS.find(x => String(x.id) === String(SELECTED_BANK_ID));
     $("bankLabel").textContent = b ? b.name : "Unknown";
   }
   
   function fillSelectWithBanks(sel, banks){
     sel.innerHTML = "";
   
     const allOpt = document.createElement("option");
     allOpt.value = "ALL";
     allOpt.textContent = "ALL BANK ACCOUNTS";
     sel.appendChild(allOpt);
   
     banks.forEach(b=>{
       const opt = document.createElement("option");
       opt.value = b.id;
       opt.textContent = b.name;
       sel.appendChild(opt);
     });
   }
   
   function renderBankSelects(){
     const sel = $("bankSelect");
     const filter = $("bankFilter");
   
     fillSelectWithBanks(sel, BANKS);
     fillSelectWithBanks(filter, BANKS);
   
     if(!SELECTED_BANK_ID) SELECTED_BANK_ID = "ALL";
     sel.value = String(SELECTED_BANK_ID);
     filter.value = String(SELECTED_BANK_ID);
   
     setBankLabel();
   }
   
   async function loadBanks(){
     const res = await fetch("/api/banks");
     BANKS = await safeJson(res);
     renderBankSelects();
   
     if(ME && ME.role === "ADMIN"){
       await loadBanksAllForAdmin();
       renderAdminBankList();
     }
   }
   
   async function loadBanksAllForAdmin(){
     const res = await fetch("/api/banks?all=1");
     BANKS_ALL = await safeJson(res);
   }
   
   async function addBank(){
     try{
       const name = ($("newBankName").value || "").trim();
       if(!name) throw new Error("Type bank name first.");
   
       const res = await fetch("/api/banks", {
         method:"POST",
         headers:{"Content-Type":"application/json"},
         body: JSON.stringify({name})
       });
   
       const out = await safeJson(res);
       if(!res.ok) throw new Error(out.error || "Failed to add bank");
   
       showMsg("bankMsg", "Bank added: " + out.name, true);
       $("newBankName").value = "";
   
       await loadBanks(); // refresh list + dropdowns
   
       SELECTED_BANK_ID = String(out.id); // auto select
       renderBankSelects();
       await refreshAll();
     }catch(err){
       showMsg("bankMsg", err.message, false);
     }
   }
   
   function renderAdminBankList(){
     const box = $("adminBankList");
     if(!box) return;
   
     if(!ME || ME.role !== "ADMIN"){
       box.innerHTML = "";
       return;
     }
   
     if(!BANKS_ALL.length){
       box.innerHTML = `<p class="sub small">No banks found.</p>`;
       return;
     }
   
     box.innerHTML = BANKS_ALL.map(b => `
       <div class="row" style="gap:8px; align-items:center; margin-bottom:10px;">
         <span class="pill ${b.active ? "" : "muted"}" style="min-width:80px; text-align:center;">
           ${b.active ? "ACTIVE" : "DISABLED"}
         </span>
   
         <input style="flex:1;" data-bank-name="${b.id}" value="${safeText(b.name)}" />
   
         <button class="btn mini" type="button" data-bank-rename="${b.id}">Rename</button>
   
         ${
           b.active
             ? `<button class="btn mini danger" type="button" data-bank-del="${b.id}">Delete</button>`
             : `<button class="btn mini" type="button" disabled>Deleted</button>`
         }
       </div>
     `).join("");
   
     // Rename
     box.querySelectorAll("[data-bank-rename]").forEach(btn=>{
       btn.addEventListener("click", async ()=>{
         try{
           const id = btn.getAttribute("data-bank-rename");
           const input = box.querySelector(`[data-bank-name="${id}"]`);
           const newName = (input.value || "").trim();
           if(!newName) throw new Error("Bank name cannot be empty.");
   
           const res = await fetch(`/api/banks/${id}`,{
             method:"PUT",
             headers:{"Content-Type":"application/json"},
             body: JSON.stringify({name:newName})
           });
   
           const out = await safeJson(res);
           if(!res.ok) throw new Error(out.error || "Rename failed");
   
           showMsg("bankMsg", "Renamed: " + out.name, true);
   
           await loadBanks(); // refresh everything
         }catch(err){
           showMsg("bankMsg", err.message, false);
         }
       });
     });
   
     // Delete (disable)
     box.querySelectorAll("[data-bank-del]").forEach(btn=>{
       btn.addEventListener("click", async ()=>{
         try{
           const id = btn.getAttribute("data-bank-del");
           const ok = confirm("Delete this bank? (Safe: it will be disabled)");
           if(!ok) return;
   
           const res = await fetch(`/api/banks/${id}`, { method:"DELETE" });
           const out = await safeJson(res);
           if(!res.ok) throw new Error(out.error || "Delete failed");
   
           showMsg("bankMsg", "Bank disabled (deleted).", true);
   
           // If selected bank was deleted -> go ALL
           if(String(SELECTED_BANK_ID) === String(id)){
             SELECTED_BANK_ID = "ALL";
           }
   
           await loadBanks();
           await refreshAll();
         }catch(err){
           showMsg("bankMsg", err.message, false);
         }
       });
     });
   }
   
   /* ---------- Finance Table ---------- */
   function fillFormFromRow(r){
     $("type").value=r.type;
     $("clientName").value=r.client_name;
     $("category").value=r.category;
     $("description").value=safeText(r.description);
     $("currency").value=r.currency;
     $("amount").value=r.amount;
     $("paymentType").value=r.payment_type;
     $("status").value=r.status;
     $("proofOfPayment").value=r.proof_of_payment;
     $("invoiceRef").value=safeText(r.invoice_ref);
     $("poNumber").value=safeText(r.po_number);
     $("quotationNumber").value=safeText(r.quotation_number);
     $("paidDate").value=safeText(r.paid_date);
     $("folderLink").value=safeText(r.folder_link);
     $("proofLink").value=safeText(r.proof_link);
     $("invoiceLink").value=safeText(r.invoice_link);
     $("quotationLink").value=safeText(r.quotation_link);
   
     if(r.bank_id !== undefined && r.bank_id !== null && r.bank_id !== ""){
       SELECTED_BANK_ID = String(r.bank_id);
       $("bankSelect").value = String(SELECTED_BANK_ID);
       $("bankFilter").value = String(SELECTED_BANK_ID);
       setBankLabel();
     }
   }
   
   function resetForm(){
     EDITING_ID=null;
     $("financeForm").reset();
     if(ME) $("createdBy").value=ME.user;
   
     $("bankSelect").value = String(SELECTED_BANK_ID);
     setBankLabel();
     showMsg("msg","Ready.",true);
   }
   
   function buildFinanceUrl(){
     const type = $("typeFilter").value;
     const params = new URLSearchParams();
   
     params.set("bank_id", String(SELECTED_BANK_ID));
     if(type !== "ALL") params.set("type", type);
     if(FILTER_MONTH) params.set("month", FILTER_MONTH);
   
     const qs = params.toString();
     return "/api/finance" + (qs ? `?${qs}` : "");
   }
   
   async function loadTable(){
     const res = await fetch(buildFinanceUrl());
     const data = await safeJson(res);
   
     const body=$("financeTableBody");
     body.innerHTML="";
   
     data.forEach(r=>{
       const canEdit = !!ME;
       const canDelete = !!ME;
   
       const clientLink = normalizeUrl(r.folder_link);
       const clientCell = clientLink
         ? `<a class="linkText" href="${clientLink}" target="_blank" rel="noopener">${safeText(r.client_name)}</a>`
         : safeText(r.client_name);
   
       const bankCell = safeText(r.bank_name || "");
   
       const tr=document.createElement("tr");
       tr.innerHTML=`
         <td>${r.id}</td>
         <td>${bankCell}</td>
         <td><span class="badge ${r.type==="INCOME"?"good":"warn"}">${r.type}</span></td>
         <td>${clientCell}</td>
         <td>${safeText(r.category)}</td>
         <td class="muted">${safeText(r.description)}</td>
         <td><b>${safeText(r.currency)} ${Number(r.amount).toFixed(2)}</b></td>
         <td>${safeText(r.payment_type)}</td>
         <td><span class="pill">${safeText(r.status)}</span></td>
         <td>${openLink(r.proof_of_payment, r.proof_link)}</td>
         <td>${openLink("OPEN", r.invoice_link)}</td>
         <td class="mono">${safeText(r.po_number)}</td>
         <td>${openLink(safeText(r.quotation_number || "OPEN"), r.quotation_link)}</td>
         <td class="mono">${safeText(r.paid_date)}</td>
         <td class="mono">${safeText(r.created_at)}</td>
         <td>${safeText(r.created_by)}</td>
         <td class="mono">${safeText(r.edited_at)}</td>
         <td>${safeText(r.edited_by)}</td>
         <td class="actionCell">
           ${canEdit ? `<button class="btn mini" data-edit="${r.id}">Edit</button>` : ``}
           ${canDelete ? `<button class="btn mini danger" data-del="${r.id}">Delete</button>` : ``}
         </td>
       `;
       body.appendChild(tr);
   
       if(canEdit){
         tr.querySelector(`[data-edit="${r.id}"]`).addEventListener("click", ()=>{
           EDITING_ID=r.id;
           fillFormFromRow(r);
           showMsg("msg",`Editing Record ID ${r.id}. Make changes and click Save Record.`,true);
           window.scrollTo({top:0, behavior:"smooth"});
         });
       }
   
       if(canDelete){
         tr.querySelector(`[data-del="${r.id}"]`).addEventListener("click", async ()=>{
           const ok = confirm(`Delete record ID ${r.id}? (It will go to Trash)`);
           if(!ok) return;
   
           const delRes = await fetch(`/api/finance/${r.id}`, { method:"DELETE" });
           const out = await safeJson(delRes);
           if(!delRes.ok) return showMsg("msg", out.error || "Delete failed", false);
   
           showMsg("msg",`Deleted record ID ${r.id}.`, true);
           await refreshAll();
         });
       }
     });
   }
   
   /* ---------- Summary ---------- */
   async function loadSummary(){
     // FIX: do NOT force INCOME when ALL
     const tf = $("typeFilter").value; // ALL / INCOME / OUTCOME
     const params = new URLSearchParams({ group: "month", bank_id: String(SELECTED_BANK_ID) });
     if(tf === "INCOME" || tf === "OUTCOME") params.set("type", tf);
     if(FILTER_MONTH) params.set("month", FILTER_MONTH);
   
     const res = await fetch("/api/finance/summary?" + params.toString());
     const rows = await safeJson(res);
   
     const tbody=$("summaryBody");
     tbody.innerHTML="";
     rows.forEach(r=>{
       const tr=document.createElement("tr");
       tr.innerHTML=`
         <td>${safeText(r.key)}</td>
         <td>${safeText(r.currency)}</td>
         <td><b>${Number(r.total).toFixed(2)}</b></td>
         <td class="muted">${r.count}</td>
       `;
       tbody.appendChild(tr);
     });
   }
   
   /* ---------- Month Balance ---------- */
   async function loadBalance(){
     const month = ($("balMonth").value || "").trim();
     const currency = $("balCurrency").value;
   
     if(!month){
       showMsg("balMsg","Enter Month like 2026-01 then click Load.", true);
       return;
     }
   
     if(String(SELECTED_BANK_ID).toUpperCase() === "ALL"){
       showMsg("balMsg","Please select a specific bank to load/save balance (ALL is not allowed).", false);
       return;
     }
   
     const url = `/api/balance?month=${encodeURIComponent(month)}&currency=${encodeURIComponent(currency)}&bank_id=${encodeURIComponent(String(SELECTED_BANK_ID))}`;
     const res = await fetch(url);
     const out = await safeJson(res);
   
     if(out.data){
       $("balAmount").value = out.data.closing_balance;
       $("balNote").value = out.data.note || "";
       showMsg("balMsg",`Loaded: ${month} ${currency}`, true);
     } else {
       $("balAmount").value = "";
       $("balNote").value = "";
       showMsg("balMsg",`No balance saved for ${month} ${currency}. You can add now.`, true);
     }
   }
   
   async function saveBalance(){
     try{
       const month = ($("balMonth").value || "").trim();
       if(!month) throw new Error("Enter Month like 2026-01 first.");
   
       if(String(SELECTED_BANK_ID).toUpperCase() === "ALL"){
         throw new Error("Please select a specific bank before saving balance (ALL is not allowed).");
       }
   
       const payload = {
         bank_id: SELECTED_BANK_ID,
         month_key: month,
         currency: $("balCurrency").value,
         closing_balance: $("balAmount").value,
         note: $("balNote").value
       };
   
       const res = await fetch("/api/balance",{
         method:"POST",
         headers:{"Content-Type":"application/json"},
         body: JSON.stringify(payload)
       });
   
       const out = await safeJson(res);
       if(!res.ok) throw new Error(out.error || "Failed to save balance");
       showMsg("balMsg","Balance saved.", true);
     }catch(err){
       showMsg("balMsg", err.message, false);
     }
   }
   
   /* ---------- Create/Update Finance ---------- */
   function getPayloadFromForm(){
     return {
       bank_id: SELECTED_BANK_ID,
       type:$("type").value,
       client_name:$("clientName").value.trim(),
       category:$("category").value.trim(),
       description:$("description").value.trim(),
       currency:$("currency").value,
       amount:$("amount").value,
       payment_type:$("paymentType").value,
       status:$("status").value,
       proof_of_payment:$("proofOfPayment").value,
       invoice_ref:$("invoiceRef").value.trim(),
       po_number:$("poNumber").value.trim(),
       quotation_number:$("quotationNumber").value.trim(),
       paid_date:$("paidDate").value,
       folder_link:$("folderLink").value.trim(),
       proof_link:$("proofLink").value.trim(),
       invoice_link:$("invoiceLink").value.trim(),
       quotation_link:$("quotationLink").value.trim(),
     };
   }
   
   async function createRecord(payload){
     // block ALL
     if(String(payload.bank_id).toUpperCase() === "ALL"){
       throw new Error("Please select a bank account before saving a record.");
     }
   
     payload.folder_link = normalizeUrl(payload.folder_link);
     payload.proof_link = normalizeUrl(payload.proof_link);
     payload.invoice_link = normalizeUrl(payload.invoice_link);
     payload.quotation_link = normalizeUrl(payload.quotation_link);
   
     const res=await fetch("/api/finance",{
       method:"POST",
       headers:{"Content-Type":"application/json"},
       body:JSON.stringify(payload)
     });
     const out = await safeJson(res);
     if(!res.ok) throw new Error(out.error || "Failed to save");
   }
   
   async function updateRecord(id, payload){
     if(String(payload.bank_id).toUpperCase() === "ALL"){
       throw new Error("Please select a bank account before updating a record.");
     }
   
     payload.folder_link = normalizeUrl(payload.folder_link);
     payload.proof_link = normalizeUrl(payload.proof_link);
     payload.invoice_link = normalizeUrl(payload.invoice_link);
     payload.quotation_link = normalizeUrl(payload.quotation_link);
   
     const res=await fetch(`/api/finance/${id}`,{
       method:"PUT",
       headers:{"Content-Type":"application/json"},
       body:JSON.stringify(payload)
     });
     const out = await safeJson(res);
     if(!res.ok) throw new Error(out.error || "Failed to update");
   }
   
   async function refreshAll(){
     await loadTable();
     await loadSummary();
   }
   
   /* ---------- events ---------- */
   $("financeForm").addEventListener("submit", async (e)=>{
     e.preventDefault();
     try{
       const payload=getPayloadFromForm();
   
       if(EDITING_ID){
         await updateRecord(EDITING_ID, payload);
         showMsg("msg",`Updated record ID ${EDITING_ID}.`,true);
       } else {
         await createRecord(payload);
         showMsg("msg","Saved new record.",true);
       }
   
       await refreshAll();
       resetForm();
     }catch(err){
       showMsg("msg", err.message, false);
     }
   });
   
   $("refreshBtn").addEventListener("click", refreshAll);
   
   $("typeFilter").addEventListener("change", refreshAll);
   
   $("viewMonthBtn").addEventListener("click", async ()=>{
     FILTER_MONTH = ($("monthPick").value || "").trim();
     await refreshAll();
   });
   
   /* EXPORT FIX: do NOT force INCOME when ALL */
   $("exportBtn").addEventListener("click", ()=>{
     const tf = $("typeFilter").value; // ALL / INCOME / OUTCOME
     const params = new URLSearchParams({ bank_id: String(SELECTED_BANK_ID) });
   
     if(tf === "INCOME" || tf === "OUTCOME") params.set("type", tf);
     if(FILTER_MONTH) params.set("month", FILTER_MONTH);
   
     window.location.href = `/api/finance/export.csv?${params.toString()}`;
   });
   
   $("loadBalanceBtn").addEventListener("click", loadBalance);
   $("saveBalanceBtn").addEventListener("click", saveBalance);
   
   /* Bank selector change (left form) */
   $("bankSelect").addEventListener("change", async ()=>{
     SELECTED_BANK_ID = $("bankSelect").value;
     $("bankFilter").value = String(SELECTED_BANK_ID);
     setBankLabel();
     resetForm();
     await refreshAll();
     showMsg("balMsg","Enter Month like 2026-01 then click Load.", true);
   });
   
   /* Bank filter change (right panel) */
   $("bankFilter").addEventListener("change", async ()=>{
     SELECTED_BANK_ID = $("bankFilter").value;
     $("bankSelect").value = String(SELECTED_BANK_ID);
     setBankLabel();
     resetForm();
     await refreshAll();
     showMsg("balMsg","Enter Month like 2026-01 then click Load.", true);
   });
   
   /* Admin add bank */
   const addBtn = $("addBankBtn");
   if(addBtn){
     addBtn.addEventListener("click", addBank);
   }
   
   // init
   (async ()=>{
     await getMe();
     await loadBanks();
     await refreshAll();
     showMsg("msg","Ready.",true);
     showMsg("balMsg","Enter Month like 2026-01 then click Load.", true);
   })();
   