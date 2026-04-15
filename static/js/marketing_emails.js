const $ = (id) => document.getElementById(id);

let ME = {
  options: {
    categories: [],
    countries: [],
    verification_statuses: [],
    export_statuses: [],
    employees: [],
    can_download: false,
    is_admin: false
  },
  leads: [],
  exports: [],
  permissions: [],
  requests: [],
  logs: [],
  selectedLeadIds: new Set(),
  editLeadId: null,
  editExportId: null,
  searchTimer: null
};

async function safeJson(res) {
  try {
    return await res.json();
  } catch {
    return {};
  }
}

async function api(url, opts = {}) {
  const res = await fetch(url, { credentials: "same-origin", ...opts });
  const data = await safeJson(res);
  if (!res.ok) throw new Error(data.error || "Request failed");
  return data;
}

function esc(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function showMsg(id, text, ok = true) {
  const el = $(id);
  if (!el) return;
  el.textContent = text || "";
  el.className = "msg " + (ok ? "ok" : "bad");
}

function normalizeLink(url) {
  const raw = String(url || "").trim();
  if (!raw) return "";
  if (/^https?:\/\//i.test(raw)) return raw;
  return "https://" + raw;
}

function statusClass(status) {
  if (status === "Confirmed Ready" || status === "Checked" || status === "APPROVED" || status === "Confirmed" || status === "Completed") {
    return "good";
  }
  if (status === "Invalid" || status === "Duplicate" || status === "DENIED" || status === "Blocked") {
    return "bad";
  }
  return "warn";
}

function setOptions(selectId, items, opts = {}) {
  const el = $(selectId);
  if (!el) return;

  const selected = opts.selected || el.value || "";
  let html = "";

  if (opts.includeAll) {
    html += `<option value="ALL">All</option>`;
  }
  if (opts.includeBlank) {
    html += `<option value="">Select</option>`;
  }

  html += (items || []).map((item) => {
    const value = typeof item === "string" ? item : item.value;
    const label = typeof item === "string" ? item : item.label;
    return `<option value="${esc(value)}">${esc(label)}</option>`;
  }).join("");

  if (opts.includeCustom) {
    html += `<option value="__CUSTOM__">Custom</option>`;
  }

  el.innerHTML = html;
  if ([...el.options].some((option) => option.value === selected)) {
    el.value = selected;
  }
}

function refreshOptionControls() {
  setOptions("leadCategory", ME.options.categories, { includeBlank: true, includeCustom: true });
  setOptions("leadCountry", ME.options.countries, { includeBlank: true, includeCustom: true });
  setOptions("categoryFilter", ME.options.categories, { includeAll: true });
  setOptions("countryFilter", ME.options.countries, { includeAll: true });
  setOptions("statusFilter", ME.options.verification_statuses, { includeAll: true });
  setOptions("bulkStatusSelect", ME.options.verification_statuses, { includeBlank: true });
  setOptions("exportStatus", ME.options.export_statuses, { includeBlank: false });

  const employees = (ME.options.employees || []).map((emp) => ({
    value: emp.username,
    label: emp.full_name ? `${emp.username} - ${emp.full_name}` : emp.username
  }));
  setOptions("employeeFilter", employees, { includeAll: true });

  $("scopeText").textContent = ME.options.is_admin
    ? "Admin view: all leads and export records."
    : "Employee view: only your own leads and export records.";

  updateDownloadBadge();
}

function updateDownloadBadge() {
  const badge = $("downloadStateBadge");
  if (!badge) return;

  if (ME.options.is_admin) {
    badge.textContent = "Download: admin";
    badge.className = "badge good";
    return;
  }

  if (ME.options.can_download) {
    badge.textContent = "Download: allowed";
    badge.className = "badge good";
  } else {
    badge.textContent = "Download: blocked";
    badge.className = "badge warn";
  }
}

function toggleCustomFields() {
  const catWrap = $("customCategoryWrap");
  const countryWrap = $("customCountryWrap");
  if (catWrap) catWrap.style.display = $("leadCategory")?.value === "__CUSTOM__" ? "" : "none";
  if (countryWrap) countryWrap.style.display = $("leadCountry")?.value === "__CUSTOM__" ? "" : "none";
}

function getSelectedCategory() {
  if ($("leadCategory")?.value === "__CUSTOM__") {
    return $("customCategory")?.value || "";
  }
  return $("leadCategory")?.value || "";
}

function getSelectedCountry() {
  if ($("leadCountry")?.value === "__CUSTOM__") {
    return $("customCountry")?.value || "";
  }
  return $("leadCountry")?.value || "";
}

function setLeadCategory(value) {
  if (!value) {
    $("leadCategory").value = "";
    $("customCategory").value = "";
  } else if ((ME.options.categories || []).includes(value)) {
    $("leadCategory").value = value;
    $("customCategory").value = "";
  } else {
    $("leadCategory").value = "__CUSTOM__";
    $("customCategory").value = value;
  }
  toggleCustomFields();
}

function setLeadCountry(value) {
  if (!value) {
    $("leadCountry").value = "";
    $("customCountry").value = "";
  } else if ((ME.options.countries || []).includes(value)) {
    $("leadCountry").value = value;
    $("customCountry").value = "";
  } else {
    $("leadCountry").value = "__CUSTOM__";
    $("customCountry").value = value;
  }
  toggleCustomFields();
}

function clearLeadForm() {
  ME.editLeadId = null;
  $("leadFormTitle").textContent = "Add Lead";
  $("companyName").value = "";
  $("leadEmail").value = "";
  $("leadWebsite").value = "";
  $("leadNote").value = "";
  setLeadCategory("");
  setLeadCountry("");
  $("saveLeadBtn").disabled = false;
  $("updateLeadBtn").disabled = true;
  showMsg("leadFormMsg", "");
}

function fillLeadForm(row) {
  ME.editLeadId = Number(row.id);
  $("leadFormTitle").textContent = "Edit Lead";
  $("companyName").value = row.company_name || "";
  $("leadEmail").value = row.email || "";
  $("leadWebsite").value = row.website || "";
  $("leadNote").value = row.note || "";
  setLeadCategory(row.category || "");
  setLeadCountry(row.country || "");
  $("saveLeadBtn").disabled = true;
  $("updateLeadBtn").disabled = false;
  showMsg("leadFormMsg", "Lead loaded for editing.", true);
}

function getLeadPayload() {
  return {
    company_name: $("companyName")?.value || "",
    email: $("leadEmail")?.value || "",
    website: $("leadWebsite")?.value || "",
    category: getSelectedCategory(),
    country: getSelectedCountry(),
    note: $("leadNote")?.value || ""
  };
}

async function loadOptions() {
  const out = await api("/api/marketing-emails/options");
  ME.options = out.data || ME.options;
  refreshOptionControls();
}

function getLeadFilters() {
  const params = new URLSearchParams();
  const q = $("searchInput")?.value.trim() || "";
  const employee = $("employeeFilter")?.value || "";
  const category = $("categoryFilter")?.value || "";
  const country = $("countryFilter")?.value || "";
  const status = $("statusFilter")?.value || "";

  if (q) params.set("q", q);
  if (employee) params.set("employee", employee);
  if (category) params.set("category", category);
  if (country) params.set("country", country);
  if (status) params.set("status", status);
  return params;
}

async function loadLeads() {
  const params = getLeadFilters();
  const out = await api(`/api/marketing-emails/leads?${params.toString()}`);
  ME.leads = out.data || [];
  renderLeads();
}

function renderLeads() {
  const body = $("leadTableBody");
  if (!body) return;

  if (!ME.leads.length) {
    body.innerHTML = `<tr><td colspan="12" class="me-muted-cell">No leads found.</td></tr>`;
    updateSelectedCount();
    return;
  }

  body.innerHTML = ME.leads.map((row) => {
    const checked = ME.selectedLeadIds.has(Number(row.id)) ? "checked" : "";
    const website = normalizeLink(row.website || "");
    const status = row.verification_status || "Not Checked Yet";
    const statusHtml = ME.options.is_admin
      ? `<select class="smallSelect" data-status-lead="${row.id}">
          ${(ME.options.verification_statuses || []).map((s) => `<option value="${esc(s)}" ${s === status ? "selected" : ""}>${esc(s)}</option>`).join("")}
        </select>`
      : `<span class="me-status ${statusClass(status)}">${esc(status)}</span>`;

    return `
      <tr>
        <td><input type="checkbox" data-lead-check="${row.id}" ${checked} /></td>
        <td>
          <b>${esc(row.company_name || "-")}</b>
          ${row.note ? `<div class="me-muted-cell">${esc(row.note)}</div>` : ""}
        </td>
        <td><a class="me-link" href="mailto:${esc(row.email)}">${esc(row.email)}</a></td>
        <td>
          ${website ? `<a class="me-link" href="${esc(website)}" target="_blank" rel="noopener noreferrer">Open Website</a>` : `<span class="me-muted-cell">No website</span>`}
        </td>
        <td>${esc(row.category)}</td>
        <td>${esc(row.country)}</td>
        <td>${esc(row.created_by)}</td>
        <td>${esc(row.edited_by || "-")}</td>
        <td>${esc(row.edited_at || "-")}</td>
        <td>${statusHtml}</td>
        <td>${esc(row.created_at || "")}</td>
        <td>
          <div class="me-actions">
            <button class="btn ghost mini" type="button" data-edit-lead="${row.id}">Edit</button>
            ${website ? `<a class="btn ghost mini" href="${esc(website)}" target="_blank" rel="noopener noreferrer">Compare</a>` : ""}
          </div>
        </td>
      </tr>
    `;
  }).join("");

  document.querySelectorAll("[data-lead-check]").forEach((box) => {
    box.addEventListener("change", () => {
      const id = Number(box.dataset.leadCheck);
      if (box.checked) {
        ME.selectedLeadIds.add(id);
      } else {
        ME.selectedLeadIds.delete(id);
      }
      updateSelectedCount();
    });
  });

  document.querySelectorAll("[data-edit-lead]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const id = Number(btn.dataset.editLead);
      const row = ME.leads.find((item) => Number(item.id) === id);
      if (row) fillLeadForm(row);
    });
  });

  document.querySelectorAll("[data-status-lead]").forEach((select) => {
    select.addEventListener("change", async () => {
      try {
        await api(`/api/marketing-emails/leads/${select.dataset.statusLead}/verify`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ verification_status: select.value })
        });
        await loadLeads();
        showMsg("leadTableMsg", "Verification status updated.", true);
      } catch (err) {
        showMsg("leadTableMsg", err.message, false);
      }
    });
  });

  updateSelectedCount();
}

function updateSelectedCount() {
  const count = ME.selectedLeadIds.size;
  const el = $("selectedLeadCount");
  if (el) el.textContent = `${count} selected`;
}

async function saveLead() {
  try {
    const out = await api("/api/marketing-emails/leads", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(getLeadPayload())
    });
    clearLeadForm();
    await loadOptions();
    await loadLeads();
    showMsg("leadFormMsg", out.message || "Email saved successfully", true);
  } catch (err) {
    showMsg("leadFormMsg", err.message, false);
  }
}

async function updateLead() {
  if (!ME.editLeadId) {
    showMsg("leadFormMsg", "Select a lead first.", false);
    return;
  }

  try {
    const out = await api(`/api/marketing-emails/leads/${ME.editLeadId}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(getLeadPayload())
    });
    clearLeadForm();
    await loadOptions();
    await loadLeads();
    showMsg("leadFormMsg", out.message || "Lead updated", true);
  } catch (err) {
    showMsg("leadFormMsg", err.message, false);
  }
}

async function bulkVerify() {
  const status = $("bulkStatusSelect")?.value || "";
  const lead_ids = Array.from(ME.selectedLeadIds);
  if (!lead_ids.length) {
    showMsg("leadTableMsg", "Select at least one lead.", false);
    return;
  }
  if (!status) {
    showMsg("leadTableMsg", "Choose a verification status.", false);
    return;
  }

  try {
    await api("/api/marketing-emails/leads/bulk-verify", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ lead_ids, verification_status: status })
    });
    await loadLeads();
    showMsg("leadTableMsg", "Selected leads updated.", true);
  } catch (err) {
    showMsg("leadTableMsg", err.message, false);
  }
}

function clearExportForm() {
  ME.editExportId = null;
  $("exportFileName").value = "";
  $("exportCategory").value = "";
  $("exportCountry").value = "";
  $("exportStatus").value = (ME.options.export_statuses || [])[0] || "Pending";
  $("firstSendDate").value = "";
  $("secondSendDate").value = "";
  $("exportNotes").value = "";
  $("createExportBtn").disabled = false;
  $("updateExportBtn").disabled = true;
  showMsg("exportFormMsg", "");
}

function fillExportForm(row) {
  ME.editExportId = Number(row.id);
  $("exportFileName").value = row.file_name || "";
  $("exportCategory").value = row.category || "";
  $("exportCountry").value = row.country || "";
  $("exportStatus").value = row.confirmation_status || "Pending";
  $("firstSendDate").value = row.first_send_date || "";
  $("secondSendDate").value = row.second_send_date || "";
  $("exportNotes").value = row.notes || "";
  $("createExportBtn").disabled = true;
  $("updateExportBtn").disabled = false;
  showMsg("exportFormMsg", "Export file loaded for editing.", true);
}

function getExportPayload() {
  return {
    file_name: $("exportFileName")?.value || "",
    category: $("exportCategory")?.value || "",
    country: $("exportCountry")?.value || "",
    confirmation_status: $("exportStatus")?.value || "Pending",
    first_send_date: $("firstSendDate")?.value || "",
    second_send_date: $("secondSendDate")?.value || "",
    notes: $("exportNotes")?.value || "",
    lead_ids: Array.from(ME.selectedLeadIds)
  };
}

function getExportFilters() {
  const params = new URLSearchParams();
  const employee = $("employeeFilter")?.value || "";
  const category = $("categoryFilter")?.value || "";
  const country = $("countryFilter")?.value || "";
  if (employee) params.set("employee", employee);
  if (category) params.set("category", category);
  if (country) params.set("country", country);
  return params;
}

async function loadExports() {
  const out = await api(`/api/marketing-emails/exports?${getExportFilters().toString()}`);
  ME.exports = out.data || [];
  renderExports();
}

function renderExports() {
  const body = $("exportTableBody");
  if (!body) return;

  if (!ME.exports.length) {
    body.innerHTML = `<tr><td colspan="11" class="me-muted-cell">No export file records found.</td></tr>`;
    return;
  }

  body.innerHTML = ME.exports.map((row) => {
    const status = row.confirmation_status || "Pending";
    return `
      <tr>
        <td><b>${esc(row.file_name)}</b></td>
        <td>${esc(row.category || "-")}</td>
        <td>${esc(row.country || "-")}</td>
        <td>${esc(row.lead_count || 0)}</td>
        <td><span class="me-status ${statusClass(status)}">${esc(status)}</span></td>
        <td>${esc(row.first_send_date || "-")}</td>
        <td>${esc(row.second_send_date || "-")}</td>
        <td>${esc(row.notes || "-")}</td>
        <td>${esc(row.created_by)}</td>
        <td>${esc(row.created_at)}</td>
        <td>
          <div class="me-actions">
            <button class="btn ghost mini" type="button" data-edit-export="${row.id}">Edit</button>
            <button class="btn mini" type="button" data-download-export="${row.id}">Download</button>
          </div>
        </td>
      </tr>
    `;
  }).join("");

  document.querySelectorAll("[data-edit-export]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const row = ME.exports.find((item) => Number(item.id) === Number(btn.dataset.editExport));
      if (row) fillExportForm(row);
    });
  });

  document.querySelectorAll("[data-download-export]").forEach((btn) => {
    btn.addEventListener("click", () => downloadExport(Number(btn.dataset.downloadExport)));
  });
}

async function createExport() {
  if (!ME.selectedLeadIds.size) {
    showMsg("exportFormMsg", "Select leads from the Leads tab first.", false);
    return;
  }

  try {
    const payload = getExportPayload();
    const firstLead = ME.leads.find((lead) => ME.selectedLeadIds.has(Number(lead.id)));
    if (firstLead) {
      payload.category = payload.category || firstLead.category || "";
      payload.country = payload.country || firstLead.country || "";
    }

    const out = await api("/api/marketing-emails/exports", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    clearExportForm();
    await loadExports();
    showMsg("exportFormMsg", out.message || "Export file record created", true);
  } catch (err) {
    showMsg("exportFormMsg", err.message, false);
  }
}

async function updateExport() {
  if (!ME.editExportId) {
    showMsg("exportFormMsg", "Select an export file first.", false);
    return;
  }

  try {
    const out = await api(`/api/marketing-emails/exports/${ME.editExportId}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(getExportPayload())
    });
    clearExportForm();
    await loadExports();
    showMsg("exportFormMsg", out.message || "Export file updated", true);
  } catch (err) {
    showMsg("exportFormMsg", err.message, false);
  }
}

async function downloadExport(exportId) {
  try {
    const res = await fetch(`/api/marketing-emails/exports/${exportId}/download`, {
      credentials: "same-origin"
    });

    if (!res.ok) {
      const data = await safeJson(res);
      throw new Error(data.error || "Download failed");
    }

    const blob = await res.blob();
    const row = ME.exports.find((item) => Number(item.id) === exportId);
    const fileName = row?.file_name || "marketing_emails.txt";
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = fileName;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
    await loadDownloadLogs();
    showMsg("exportTableMsg", "Download started.", true);
  } catch (err) {
    showMsg("exportTableMsg", err.message, false);
  }
}

async function loadPermissions() {
  const out = await api("/api/marketing-emails/download-permissions");
  ME.permissions = out.data || [];
  renderPermissions();
}

function renderPermissions() {
  const body = $("permissionTableBody");
  if (!body) return;

  if (!ME.permissions.length) {
    body.innerHTML = `<tr><td colspan="6" class="me-muted-cell">No employees found.</td></tr>`;
    return;
  }

  body.innerHTML = ME.permissions.map((row) => {
    const allowed = Number(row.can_download || 0) === 1;
    return `
      <tr>
        <td><b>${esc(row.username)}</b><div class="me-muted-cell">${esc(row.full_name || "")}</div></td>
        <td><span class="me-status ${allowed ? "good" : "warn"}">${allowed ? "Allowed" : "Blocked"}</span></td>
        <td>${esc(row.notes || "-")}</td>
        <td>${esc(row.updated_by || row.created_by || "-")}</td>
        <td>${esc(row.updated_at || row.created_at || "-")}</td>
        <td>
          <div class="me-actions">
            <button class="btn mini" type="button" data-allow-download="${row.user_id}">Allow</button>
            <button class="btn ghost mini" type="button" data-block-download="${row.user_id}">Block</button>
          </div>
        </td>
      </tr>
    `;
  }).join("");

  document.querySelectorAll("[data-allow-download]").forEach((btn) => {
    btn.addEventListener("click", () => setDownloadPermission(Number(btn.dataset.allowDownload), 1));
  });

  document.querySelectorAll("[data-block-download]").forEach((btn) => {
    btn.addEventListener("click", () => setDownloadPermission(Number(btn.dataset.blockDownload), 0));
  });
}

async function setDownloadPermission(userId, canDownload) {
  const note = prompt("Permission note", "");
  try {
    await api("/api/marketing-emails/download-permissions", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ user_id: userId, can_download: canDownload, notes: note || "" })
    });
    await loadPermissions();
    showMsg("requestMsg", "Download permission updated.", true);
  } catch (err) {
    showMsg("requestMsg", err.message, false);
  }
}

async function loadRequests() {
  const out = await api("/api/marketing-emails/download-requests");
  ME.requests = out.data || [];
  renderRequests();
}

function renderRequests() {
  const body = $("requestTableBody");
  if (!body) return;

  if (!ME.requests.length) {
    body.innerHTML = `<tr><td colspan="7" class="me-muted-cell">No download requests found.</td></tr>`;
    return;
  }

  body.innerHTML = ME.requests.map((row) => {
    const status = row.status || "PENDING";
    const pendingActions = status === "PENDING" && ME.options.is_admin
      ? `<button class="btn mini" type="button" data-approve-request="${row.id}">Approve</button>
         <button class="btn ghost mini" type="button" data-deny-request="${row.id}">Deny</button>`
      : `<span class="me-muted-cell">${status === "PENDING" ? "Pending" : "Decided"}</span>`;

    return `
      <tr>
        <td><b>${esc(row.username || row.user_id)}</b><div class="me-muted-cell">${esc(row.full_name || "")}</div></td>
        <td>${esc(row.request_note || "-")}</td>
        <td><span class="me-status ${statusClass(status)}">${esc(status)}</span></td>
        <td>${esc(row.requested_at || "-")}</td>
        <td>${esc(row.decided_by || "-")}</td>
        <td>${esc(row.decision_note || "-")}</td>
        <td><div class="me-actions">${pendingActions}</div></td>
      </tr>
    `;
  }).join("");

  document.querySelectorAll("[data-approve-request]").forEach((btn) => {
    btn.addEventListener("click", () => decideRequest(Number(btn.dataset.approveRequest), "APPROVED"));
  });

  document.querySelectorAll("[data-deny-request]").forEach((btn) => {
    btn.addEventListener("click", () => decideRequest(Number(btn.dataset.denyRequest), "DENIED"));
  });
}

async function requestDownloadAccess() {
  try {
    const out = await api("/api/marketing-emails/download-requests", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ request_note: $("requestNote")?.value || "" })
    });
    $("requestNote").value = "";
    await loadRequests();
    showMsg("requestMsg", out.message || "Download access request sent", true);
  } catch (err) {
    showMsg("requestMsg", err.message, false);
  }
}

async function decideRequest(requestId, status) {
  const note = prompt("Decision note", "");
  try {
    await api(`/api/marketing-emails/download-requests/${requestId}/decide`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status, decision_note: note || "" })
    });
    await loadRequests();
    await loadPermissions();
    showMsg("requestMsg", "Request updated.", true);
  } catch (err) {
    showMsg("requestMsg", err.message, false);
  }
}

async function loadDownloadLogs() {
  const out = await api("/api/marketing-emails/download-logs");
  ME.logs = out.data || [];
  renderDownloadLogs();
}

function renderDownloadLogs() {
  const body = $("downloadLogBody");
  if (!body) return;

  if (!ME.logs.length) {
    body.innerHTML = `<tr><td colspan="3" class="me-muted-cell">No download logs found.</td></tr>`;
    return;
  }

  body.innerHTML = ME.logs.map((row) => `
    <tr>
      <td><b>${esc(row.file_name)}</b></td>
      <td>${esc(row.downloaded_by)}</td>
      <td>${esc(row.downloaded_at)}</td>
    </tr>
  `).join("");
}

function switchTab(tabName) {
  document.querySelectorAll(".me-tab").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.tab === tabName);
  });
  document.querySelectorAll(".me-section").forEach((section) => {
    section.classList.toggle("active", section.id === `tab-${tabName}`);
  });
}

function scheduleSearchReload() {
  if (ME.searchTimer) clearTimeout(ME.searchTimer);
  ME.searchTimer = setTimeout(async () => {
    try {
      await loadLeads();
    } catch (err) {
      showMsg("leadTableMsg", err.message, false);
    }
  }, 250);
}

async function refreshAll() {
  try {
    await loadOptions();
    await loadLeads();
    await loadExports();
    await loadPermissions();
    await loadRequests();
    await loadDownloadLogs();
  } catch (err) {
    showMsg("leadTableMsg", err.message, false);
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  document.querySelectorAll(".me-tab").forEach((btn) => {
    btn.addEventListener("click", () => switchTab(btn.dataset.tab));
  });

  $("leadCategory")?.addEventListener("change", toggleCustomFields);
  $("leadCountry")?.addEventListener("change", toggleCustomFields);
  $("saveLeadBtn")?.addEventListener("click", saveLead);
  $("updateLeadBtn")?.addEventListener("click", updateLead);
  $("clearLeadBtn")?.addEventListener("click", clearLeadForm);
  $("applyFiltersBtn")?.addEventListener("click", async () => {
    await loadLeads();
    await loadExports();
  });
  $("refreshAllBtn")?.addEventListener("click", refreshAll);
  $("bulkVerifyBtn")?.addEventListener("click", bulkVerify);
  $("createExportBtn")?.addEventListener("click", createExport);
  $("updateExportBtn")?.addEventListener("click", updateExport);
  $("clearExportBtn")?.addEventListener("click", clearExportForm);
  $("requestDownloadBtn")?.addEventListener("click", requestDownloadAccess);
  $("searchInput")?.addEventListener("input", scheduleSearchReload);
  $("searchInput")?.addEventListener("keydown", async (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      await loadLeads();
    }
  });
  ["employeeFilter", "categoryFilter", "countryFilter", "statusFilter"].forEach((id) => {
    $(id)?.addEventListener("change", async () => {
      await loadLeads();
      await loadExports();
    });
  });

  clearLeadForm();
  await loadOptions();
  clearExportForm();
  await refreshAll();
});
