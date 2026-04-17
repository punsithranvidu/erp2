let ME = null;
let SELECTED_USER_ID = null;
let SELECTED_USER_ROW = null;

const MODULES = [
  { key: "FINANCE", label: "Finance" },
  { key: "DOCUMENT_STORAGE", label: "Document Storage" },
  { key: "USERS", label: "Employee Access" },
  { key: "CLIENTS", label: "Clients" },
  { key: "INVOICES", label: "Invoices" },
  { key: "CASH_ADVANCES", label: "Cash Advances" },
  { key: "FINANCE_TRASH", label: "Finance Trash" },
  { key: "REPORTS", label: "Reports" },
  { key: "MESSAGES", label: "Messages" },
  { key: "CALENDAR", label: "Calendar" },
  { key: "HS_CODES", label: "HS Codes" },
  { key: "WORKSHEET", label: "Worksheet" },
  { key: "ADMIN_WORKSHEET", label: "Admin Worksheet" },
  { key: "ATTENDANCE", label: "Attendance" },
  { key: "MARKETING_EMAILS", label: "Marketing Emails" },
  { key: "NOTES", label: "Notes" }
];

function showMsg(id, text, ok){
  const el = document.getElementById(id);
  if(!el) return;
  el.textContent = text || "";
  el.className = "msg " + (ok ? "ok" : "bad");
}

function setSelectedAlert(text){
  const el = document.getElementById("selectedAlert");
  if(!el) return;
  if(!text){
    el.style.display = "none";
    el.textContent = "";
    return;
  }
  el.style.display = "block";
  el.textContent = text;
}

async function safeJson(res){
  try { return await res.json(); } catch { return {}; }
}

async function api(url, opts={}){
  const res = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    credentials: "same-origin",
    ...opts
  });
  const data = await safeJson(res);
  if(!res.ok) throw new Error(data.error || "Request failed");
  return data;
}

function normalizeUrl(url){
  const u = (url || "").trim();
  if(!u) return "";
  if(/^https?:\/\//i.test(u)) return u;
  if(/^(mailto:|tel:)/i.test(u)) return u;
  return "https://" + u;
}

function rowFileCell(row){
  const name = row.file_name || "";
  const link = normalizeUrl(row.file_link || "");

  if(!name) return "";

  if(link){
    return `<a class="linkText" href="${link}" target="_blank" rel="noopener">${name}</a>`;
  }

  return name;
}

function renderPermissionModules(){
  const wrap = document.getElementById("permModules");
  if(!wrap) return;
  wrap.innerHTML = MODULES.map(m => `
    <label class="row" style="gap:8px;">
      <input type="checkbox" data-mod="${m.key}" />
      ${m.label}
    </label>
  `).join("");
}

function fillEditForm(row){
  document.getElementById("eFullName").value = row.full_name || "";
  document.getElementById("eNic").value = row.nic || "";
  document.getElementById("eJoinDate").value = row.join_date || "";
  document.getElementById("eJobRole").value = row.job_role || "";
  document.getElementById("eAddress").value = row.address || "";
  document.getElementById("eGoogleEmail").value = row.google_email || "";
  document.getElementById("eFileName").value = row.file_name || "";
  document.getElementById("eFileLink").value = row.file_link || "";

  document.getElementById("editRole").value = row.role;
  document.getElementById("editActive").value = String(row.active);
  document.getElementById("renameTo").value = "";
  document.getElementById("newPass").value = "";

  showMsg("renameMsg", "", true);
  showMsg("passMsg", "", true);
  showMsg("saveMsg", "", true);
  showMsg("permMsg", "", true);
  showMsg("dangerMsg", "", true);

  setSelectedAlert(`Selected: ${row.username} (ID ${row.id})`);
}

function setPermUI(role, permsObj){
  const box = document.getElementById("permBox");
  if(!box) return;

  const p = permsObj || {};
  const inputs = document.querySelectorAll("[data-mod]");

  if(role === "ADMIN"){
    box.style.opacity = "0.55";
    box.style.pointerEvents = "none";
    inputs.forEach(i => { i.checked = true; });
    return;
  }

  box.style.opacity = "1";
  box.style.pointerEvents = "auto";

  inputs.forEach(i => {
    const key = i.getAttribute("data-mod");
    const row = p[key] || { can_access: 0 };
    i.checked = Number(row.can_access) === 1;
  });
}

async function loadPerms(uid){
  const out = await api(`/api/users/${uid}/permissions`);
  return (out.data && out.data.permissions) ? out.data.permissions : {};
}

async function pickUserRow(row){
  SELECTED_USER_ID = row.id;
  SELECTED_USER_ROW = row;
  fillEditForm(row);
  const perms = await loadPerms(row.id);
  setPermUI(row.role, perms);
}

function renderUsers(rows){
  const body = document.getElementById("usersBody");
  body.innerHTML = "";

  rows.forEach(r => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.id}</td>
      <td><b>${r.username}</b></td>
      <td>${r.role}</td>
      <td>${r.active ? "YES" : "NO"}</td>
      <td>${r.full_name || ""}</td>
      <td>${r.google_email || ""}</td>
      <td>${rowFileCell(r)}</td>
      <td>${r.job_role || ""}</td>
      <td>${r.join_date || ""}</td>
      <td>${r.created_at || ""}</td>
      <td style="text-align:center;">
        <button class="btn mini ghost" data-act="select">Select</button>
      </td>
    `;

    tr.querySelector('[data-act="select"]').addEventListener("click", (e) => {
      e.stopPropagation();
      pickUserRow(r);
    });

    tr.style.cursor = "pointer";
    tr.addEventListener("click", () => pickUserRow(r));
    body.appendChild(tr);
  });
}

async function loadMe(){
  ME = await api("/api/me");
  if(ME.role !== "ADMIN"){
    alert("Admin only");
    location.href = "/dashboard";
  }
}

async function loadUsers(){
  const out = await api("/api/users");
  renderUsers(out.data || []);
}

async function createUser(){
  try{
    const payload = {
      username: document.getElementById("uUsername").value.trim(),
      password: document.getElementById("uPassword").value,
      role: document.getElementById("uRole").value,
      full_name: document.getElementById("uFullName").value.trim(),
      nic: document.getElementById("uNic").value.trim(),
      join_date: document.getElementById("uJoinDate").value,
      job_role: document.getElementById("uJobRole").value.trim(),
      address: document.getElementById("uAddress").value.trim(),
      google_email: document.getElementById("uGoogleEmail").value.trim().toLowerCase(),
      file_name: document.getElementById("uFileName").value.trim(),
      file_link: normalizeUrl(document.getElementById("uFileLink").value.trim())
    };
    const out = await api("/api/users", { method:"POST", body: JSON.stringify(payload) });
    showMsg("createMsg", `User created: ${out.data.username}`, true);
    document.getElementById("uUsername").value = "";
    document.getElementById("uPassword").value = "";
    document.getElementById("uFullName").value = "";
    document.getElementById("uNic").value = "";
    document.getElementById("uJoinDate").value = "";
    document.getElementById("uJobRole").value = "";
    document.getElementById("uAddress").value = "";
    document.getElementById("uGoogleEmail").value = "";
    document.getElementById("uFileName").value = "";
    document.getElementById("uFileLink").value = "";
    await loadUsers();
  }catch(e){
    showMsg("createMsg", e.message, false);
  }
}

async function renameUser(){
  try{
    if(!SELECTED_USER_ID) return showMsg("renameMsg", "Select a user first", false);
    const newUsername = document.getElementById("renameTo").value.trim();
    const out = await api(`/api/users/${SELECTED_USER_ID}/rename`, {
      method:"POST",
      body: JSON.stringify({ username: newUsername })
    });
    showMsg("renameMsg", `Renamed to: ${out.data.username}`, true);
    await loadUsers();
  }catch(e){
    showMsg("renameMsg", e.message, false);
  }
}

async function resetPassword(){
  try{
    if(!SELECTED_USER_ID) return showMsg("passMsg", "Select a user first", false);
    const pw = document.getElementById("newPass").value;
    const out = await api(`/api/users/${SELECTED_USER_ID}/password`, {
      method:"POST",
      body: JSON.stringify({ password: pw })
    });
    showMsg("passMsg", `Password updated for ${out.data.username}`, true);
  }catch(e){
    showMsg("passMsg", e.message, false);
  }
}

async function saveUser(){
  try{
    if(!SELECTED_USER_ID) return showMsg("saveMsg", "Select a user first", false);

    const payload = {
      role: document.getElementById("editRole").value,
      active: parseInt(document.getElementById("editActive").value, 10),
      full_name: document.getElementById("eFullName").value.trim(),
      nic: document.getElementById("eNic").value.trim(),
      join_date: document.getElementById("eJoinDate").value,
      job_role: document.getElementById("eJobRole").value.trim(),
      address: document.getElementById("eAddress").value.trim(),
      google_email: document.getElementById("eGoogleEmail").value.trim().toLowerCase(),
      file_name: document.getElementById("eFileName").value.trim(),
      file_link: normalizeUrl(document.getElementById("eFileLink").value.trim())
    };

    const out = await api(`/api/users/${SELECTED_USER_ID}`, {
      method:"PUT",
      body: JSON.stringify(payload)
    });

    showMsg("saveMsg", `Saved: ${out.data.username}`, true);
    const perms = await loadPerms(SELECTED_USER_ID);
    setPermUI(payload.role, perms);
    await loadUsers();
  }catch(e){
    showMsg("saveMsg", e.message, false);
  }
}

async function savePermissions(){
  try{
    if(!SELECTED_USER_ID) return showMsg("permMsg", "Select a user first", false);

    const role = document.getElementById("editRole").value;
    if(role === "ADMIN"){
      return showMsg("permMsg", "Admin has full access.", true);
    }

    const permissions = {};
    document.querySelectorAll("[data-mod]").forEach(el => {
      const key = el.getAttribute("data-mod");
      const on = el.checked;
      permissions[key] = on ? { can_access: 1, can_edit: 1 } : { can_access: 0, can_edit: 0 };
    });

    await api(`/api/users/${SELECTED_USER_ID}/permissions`, {
      method: "PUT",
      body: JSON.stringify({ permissions })
    });

    showMsg("permMsg", "Permissions saved ✅", true);
  }catch(e){
    showMsg("permMsg", e.message, false);
  }
}

async function disableUser(){
  try{
    if(!SELECTED_USER_ID) return showMsg("dangerMsg", "Select a user first", false);
    await api(`/api/users/${SELECTED_USER_ID}/disable`, { method:"POST" });
    showMsg("dangerMsg", "User disabled ✅", true);
    await loadUsers();
  }catch(e){
    showMsg("dangerMsg", e.message, false);
  }
}

async function deleteUser(){
  try{
    if(!SELECTED_USER_ID) return showMsg("dangerMsg", "Select a user first", false);
    if(!confirm("Delete permanently?")) return;
    await api(`/api/users/${SELECTED_USER_ID}`, { method:"DELETE" });
    showMsg("dangerMsg", "User deleted ✅", true);
    await loadUsers();
  }catch(e){
    showMsg("dangerMsg", e.message, false);
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  renderPermissionModules();
  await loadMe();
  await loadUsers();

  document.getElementById("refreshUsersBtn").addEventListener("click", loadUsers);
  document.getElementById("createUserBtn").addEventListener("click", createUser);
  document.getElementById("renameBtn").addEventListener("click", renameUser);
  document.getElementById("resetPassBtn").addEventListener("click", resetPassword);
  document.getElementById("saveUserBtn").addEventListener("click", saveUser);
  document.getElementById("savePermBtn").addEventListener("click", savePermissions);
  document.getElementById("disableBtn").addEventListener("click", disableUser);
  document.getElementById("deleteBtn").addEventListener("click", deleteUser);

  document.getElementById("editRole").addEventListener("change", async () => {
    if(!SELECTED_USER_ID) return;
    const role = document.getElementById("editRole").value;
    const perms = await loadPerms(SELECTED_USER_ID);
    setPermUI(role, perms);
  });
});
