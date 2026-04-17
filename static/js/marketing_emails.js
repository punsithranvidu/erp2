const $ = (id) => document.getElementById(id);

let ME = {
  options: {
    categories: [],
    countries: [],
    verification_statuses: [],
    call_statuses: [],
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
  trash: { leads: [], exports: [] },
  selectedLeadIds: new Set(),
  selectedExportIds: new Set(),
  editLeadId: null,
  editExportId: null,
  editExportExtraDates: [],
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
  if (status === "Checked" || status === "APPROVED" || status === "Confirmed" || status === "Send First Email" || status === "Send Second Email") {
    return "good";
  }
  if (status === "Invalid" || status === "DENIED" || status === "Invalid Email List" || status === "Called - Negative") {
    return "bad";
  }
  return "warn";
}

function parseExtraSendDates(value) {
  if (Array.isArray(value)) return value;
  if (!value) return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function effectiveSendRound(rowOrStatus, maybeRound) {
  const status = typeof rowOrStatus === "string" ? rowOrStatus : rowOrStatus?.confirmation_status;
  const round = typeof rowOrStatus === "string" ? maybeRound : rowOrStatus?.send_round;
  if (status === "Send First Email") return 1;
  if (status === "Send Second Email") return 2;
  if (status === "Custom Send Stage") return Math.max(3, Number(round || 3));
  return 0;
}

function ordinal(n) {
  const value = Number(n || 0);
  if (value === 1) return "First";
  if (value === 2) return "Second";
  if (value === 3) return "Third";
  if (value === 4) return "Fourth";
  if (value === 5) return "Fifth";
  if (value === 6) return "Sixth";
  if (value === 7) return "Seventh";
  if (value === 8) return "Eighth";
  if (value === 9) return "Ninth";
  if (value === 10) return "Tenth";
  return `${value}th`;
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
  setOptions("callStatusFilter", ME.options.call_statuses, { includeAll: true });
  setOptions("bulkStatusSelect", ME.options.verification_statuses, { includeBlank: true });
  setOptions("exportStatus", ME.options.export_statuses, { includeBlank: false });
  setOptions("exportStatusFilter", ME.options.export_statuses, { includeAll: true });
  setOptions("leadCallStatus", ME.options.call_statuses, { includeBlank: false });

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
  $("leadCallStatus").value = "Not Yet Called";
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
  $("leadCallStatus").value = row.call_status || "Not Yet Called";
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
    call_status: $("leadCallStatus")?.value || "Not Yet Called",
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
  const callStatus = $("callStatusFilter")?.value || "";

  if (q) params.set("q", q);
  if (employee) params.set("employee", employee);
  if (category) params.set("category", category);
  if (country) params.set("country", country);
  if (status) params.set("status", status);
  if (callStatus) params.set("call_status", callStatus);
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
    body.innerHTML = `<tr><td colspan="13" class="me-muted-cell">No leads found.</td></tr>`;
    updateLeadTotals();
    updateSelectedCount();
    setupTableScrollbars();
    return;
  }

  body.innerHTML = ME.leads.map((row) => {
    const checked = ME.selectedLeadIds.has(Number(row.id)) ? "checked" : "";
    const website = normalizeLink(row.website || "");
    const status = row.verification_status || "Not Yet Checked";
    const statusHtml = ME.options.is_admin
      ? `<select class="smallSelect" data-status-lead="${row.id}">
          ${(ME.options.verification_statuses || []).map((s) => `<option value="${esc(s)}" ${s === status ? "selected" : ""}>${esc(s)}</option>`).join("")}
        </select>`
      : `<span class="me-status ${statusClass(status)}">${esc(status)}</span>`;
    const callStatus = row.call_status || "Not Yet Called";
    const callStatusHtml = `<select class="smallSelect" data-call-status-lead="${row.id}">
      ${(ME.options.call_statuses || []).map((s) => `<option value="${esc(s)}" ${s === callStatus ? "selected" : ""}>${esc(s)}</option>`).join("")}
    </select>`;

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
        <td>${callStatusHtml}</td>
        <td>${esc(row.created_by)}</td>
        <td>${esc(row.edited_by || "-")}</td>
        <td>${esc(row.edited_at || "-")}</td>
        <td>${statusHtml}</td>
        <td>${esc(row.created_at || "")}</td>
        <td>
          <div class="me-actions">
            <button class="btn ghost mini" type="button" data-edit-lead="${row.id}">Edit</button>
            <button class="btn danger mini" type="button" data-delete-lead="${row.id}">Delete</button>
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

  document.querySelectorAll("[data-call-status-lead]").forEach((select) => {
    select.addEventListener("change", async () => {
      try {
        await api(`/api/marketing-emails/leads/${select.dataset.callStatusLead}/call-status`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ call_status: select.value })
        });
        await loadLeads();
        showMsg("leadTableMsg", "Call status updated.", true);
      } catch (err) {
        showMsg("leadTableMsg", err.message, false);
      }
    });
  });

  document.querySelectorAll("[data-delete-lead]").forEach((btn) => {
    btn.addEventListener("click", () => deleteLead(Number(btn.dataset.deleteLead)));
  });

  updateSelectedCount();
  updateLeadTotals();
  setupTableScrollbars();
}

function updateLeadTotals() {
  const el = $("leadTotalText");
  if (!el) return;
  const count = ME.leads.length;
  el.textContent = `${count} lead${count === 1 ? "" : "s"} / email${count === 1 ? "" : "s"} shown`;
}

function updateSelectedCount() {
  const count = ME.selectedLeadIds.size;
  const el = $("selectedLeadCount");
  if (el) el.textContent = `${count} selected`;

  const visibleIds = ME.leads.map((row) => Number(row.id));
  const allVisibleSelected = visibleIds.length > 0 && visibleIds.every((id) => ME.selectedLeadIds.has(id));
  const leadBox = $("leadSelectAllBox");
  if (leadBox) leadBox.checked = allVisibleSelected;
}

function selectAllVisibleLeads() {
  ME.leads.forEach((row) => ME.selectedLeadIds.add(Number(row.id)));
  renderLeads();
}

function clearLeadSelection() {
  ME.selectedLeadIds.clear();
  renderLeads();
}

async function deleteLead(leadId) {
  if (!confirm("Move this lead to trash?")) return;
  try {
    await api(`/api/marketing-emails/leads/${leadId}/delete`, { method: "POST" });
    ME.selectedLeadIds.delete(leadId);
    await loadLeads();
    await loadTrash();
    showMsg("leadTableMsg", "Lead moved to trash.", true);
  } catch (err) {
    showMsg("leadTableMsg", err.message, false);
  }
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
  ME.editExportExtraDates = [];
  $("exportFileName").value = "";
  $("exportCategory").value = "";
  $("exportCountry").value = "";
  $("exportStatus").value = (ME.options.export_statuses || [])[0] || "Pending";
  $("sendRound").value = "3";
  $("firstSendDate").value = "";
  $("secondSendDate").value = "";
  renderExtraSendDateInputs([]);
  updateSendDateVisibility();
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
  $("sendRound").value = String(Math.max(3, Number(row.send_round || 3)));
  $("firstSendDate").value = row.first_send_date || "";
  $("secondSendDate").value = row.second_send_date || "";
  ME.editExportExtraDates = parseExtraSendDates(row.extra_send_dates);
  renderExtraSendDateInputs(ME.editExportExtraDates);
  updateSendDateVisibility();
  $("exportNotes").value = row.notes || "";
  $("createExportBtn").disabled = true;
  $("updateExportBtn").disabled = false;
  showMsg("exportFormMsg", "Export file loaded for editing.", true);
}

function getExportPayload() {
  const extraDates = Array.from(document.querySelectorAll("[data-extra-send-date]")).map((input) => input.value || "");
  if (extraDates.length < ME.editExportExtraDates.length) {
    extraDates.push(...ME.editExportExtraDates.slice(extraDates.length));
  }
  return {
    file_name: $("exportFileName")?.value || "",
    category: $("exportCategory")?.value || "",
    country: $("exportCountry")?.value || "",
    confirmation_status: $("exportStatus")?.value || "Pending",
    first_send_date: $("firstSendDate")?.value || "",
    second_send_date: $("secondSendDate")?.value || "",
    send_round: Number($("sendRound")?.value || 0),
    extra_send_dates: extraDates,
    notes: $("exportNotes")?.value || "",
    lead_ids: Array.from(ME.selectedLeadIds)
  };
}

function renderExtraSendDateInputs(values = []) {
  const wrap = $("extraSendDatesWrap");
  if (!wrap) return;

  const round = effectiveSendRound($("exportStatus")?.value || "Pending", $("sendRound")?.value || 0);
  const count = Math.max(0, round - 2);
  const existing = Array.from(document.querySelectorAll("[data-extra-send-date]")).map((input) => input.value || "");
  const source = values.length ? values : (existing.length ? existing : ME.editExportExtraDates);
  const html = [];

  for (let i = 0; i < count; i += 1) {
    const stage = i + 3;
    html.push(`
      <div>
        <label for="extraSendDate${stage}">${esc(ordinal(stage))} Send Date</label>
        <input id="extraSendDate${stage}" data-extra-send-date="${stage}" type="date" value="${esc(source[i] || "")}" />
      </div>
    `);
  }

  wrap.innerHTML = html.join("");
  wrap.style.display = count > 0 ? "grid" : "none";
  wrap.style.gridTemplateColumns = "repeat(4, minmax(150px, 1fr))";
  wrap.style.gap = "10px";
}

function updateSendDateVisibility() {
  const status = $("exportStatus")?.value || "Pending";
  const round = effectiveSendRound(status, $("sendRound")?.value || 0);
  const roundWrap = $("sendRoundWrap");
  if (roundWrap) roundWrap.style.display = status === "Custom Send Stage" ? "" : "none";

  const first = $("firstSendDate")?.closest("div");
  const second = $("secondSendDate")?.closest("div");
  if (first) first.style.display = round >= 1 ? "" : "none";
  if (second) second.style.display = round >= 2 ? "" : "none";

  renderExtraSendDateInputs();
}

function getExportFilters() {
  const params = new URLSearchParams();
  const employee = $("employeeFilter")?.value || "";
  const category = $("categoryFilter")?.value || "";
  const country = $("countryFilter")?.value || "";
  const status = $("exportStatusFilter")?.value || "";
  if (employee) params.set("employee", employee);
  if (category) params.set("category", category);
  if (country) params.set("country", country);
  if (status) params.set("status", status);
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
    updateExportTotals();
    setupTableScrollbars();
    return;
  }

  body.innerHTML = ME.exports.map((row) => {
    const status = row.confirmation_status || "Pending";
    const checked = ME.selectedExportIds.has(Number(row.id)) ? "checked" : "";
    const sendDates = renderSendDatesCell(row);
    return `
      <tr>
        <td><input type="checkbox" data-export-check="${row.id}" ${checked} /></td>
        <td><b>${esc(row.file_name)}</b></td>
        <td>${esc(row.category || "-")}</td>
        <td>${esc(row.country || "-")}</td>
        <td>${esc(row.lead_count || 0)}</td>
        <td><span class="me-status ${statusClass(status)}">${esc(status)}</span></td>
        <td>${sendDates}</td>
        <td>${esc(row.notes || "-")}</td>
        <td>${esc(row.created_by)}</td>
        <td>${esc(row.created_at)}</td>
        <td>
          <div class="me-actions">
            <button class="btn ghost mini" type="button" data-edit-export="${row.id}">Edit</button>
            <button class="btn mini" type="button" data-download-export="${row.id}">Download</button>
            <button class="btn danger mini" type="button" data-delete-export="${row.id}">Delete</button>
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

  document.querySelectorAll("[data-delete-export]").forEach((btn) => {
    btn.addEventListener("click", () => deleteExport(Number(btn.dataset.deleteExport)));
  });

  document.querySelectorAll("[data-export-check]").forEach((box) => {
    box.addEventListener("change", () => {
      const id = Number(box.dataset.exportCheck);
      if (box.checked) {
        ME.selectedExportIds.add(id);
      } else {
        ME.selectedExportIds.delete(id);
      }
      updateSelectedExportCount();
    });
  });

  updateSelectedExportCount();
  updateExportTotals();
  setupTableScrollbars();
}

function renderSendDatesCell(row) {
  const round = effectiveSendRound(row);
  if (round <= 0) return `<span class="me-muted-cell">No send date</span>`;

  const dates = [];
  if (round >= 1) dates.push({ label: "First", value: row.first_send_date || "" });
  if (round >= 2) dates.push({ label: "Second", value: row.second_send_date || "" });

  const extra = parseExtraSendDates(row.extra_send_dates);
  for (let stage = 3; stage <= round; stage += 1) {
    dates.push({ label: ordinal(stage), value: extra[stage - 3] || "" });
  }

  return dates.map((item) => `
    <div><b>${esc(item.label)}:</b> ${esc(item.value || "-")}</div>
  `).join("");
}

function updateSelectedExportCount() {
  const visibleIds = ME.exports.map((row) => Number(row.id));
  const allVisibleSelected = visibleIds.length > 0 && visibleIds.every((id) => ME.selectedExportIds.has(id));
  const box = $("exportSelectAllBox");
  if (box) box.checked = allVisibleSelected;
}

function updateExportTotals() {
  const el = $("exportTotalText");
  if (!el) return;
  const fileCount = ME.exports.length;
  const emailCount = ME.exports.reduce((sum, row) => sum + Number(row.lead_count || 0), 0);
  el.textContent = `${fileCount} file${fileCount === 1 ? "" : "s"} • ${emailCount} email${emailCount === 1 ? "" : "s"}`;
}

function selectAllVisibleExports() {
  ME.exports.forEach((row) => ME.selectedExportIds.add(Number(row.id)));
  renderExports();
}

function clearExportSelection() {
  ME.selectedExportIds.clear();
  renderExports();
}

async function deleteExport(exportId) {
  if (!confirm("Move this export file record to trash?")) return;
  try {
    await api(`/api/marketing-emails/exports/${exportId}/delete`, { method: "POST" });
    ME.selectedExportIds.delete(exportId);
    await loadExports();
    await loadTrash();
    showMsg("exportTableMsg", "Export file moved to trash.", true);
  } catch (err) {
    showMsg("exportTableMsg", err.message, false);
  }
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
  renderRequests();
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
    const hadApprovedRequest = ME.requests.some((req) => (
      Number(req.user_id || 0) === Number(row.user_id || 0)
      && String(req.status || "").toUpperCase() === "APPROVED"
    ));
    const statusHtml = allowed
      ? `<span class="me-status good">Allowed</span>`
      : hadApprovedRequest
        ? `<div class="me-actions">
             <span class="me-status old good">Approved</span>
             <span class="me-status bad">Blocked / Revoked</span>
           </div>`
        : `<span class="me-status warn">Blocked</span>`;
    return `
      <tr>
        <td><b>${esc(row.username)}</b><div class="me-muted-cell">${esc(row.full_name || "")}</div></td>
        <td>${statusHtml}</td>
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
    const statusHtml = renderRequestStatus(row);
    const pendingActions = ME.options.is_admin
      ? `${status === "PENDING" ? `
           <button class="btn mini" type="button" data-approve-request="${row.id}">Approve</button>
           <button class="btn ghost mini" type="button" data-deny-request="${row.id}">Deny</button>
         ` : ""}
         <button class="btn danger mini" type="button" data-delete-request="${row.id}">Delete</button>`
      : `${status === "PENDING" ? `
           <button class="btn ghost mini" type="button" data-edit-request="${row.id}">Edit</button>
           <button class="btn danger mini" type="button" data-delete-request="${row.id}">Delete</button>
         ` : `<span class="me-muted-cell">Decided</span>`}`;

    return `
      <tr>
        <td><b>${esc(row.username || row.user_id)}</b><div class="me-muted-cell">${esc(row.full_name || "")}</div></td>
        <td>${esc(row.request_note || "-")}</td>
        <td>${statusHtml}</td>
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

  document.querySelectorAll("[data-edit-request]").forEach((btn) => {
    btn.addEventListener("click", () => editRequest(Number(btn.dataset.editRequest)));
  });

  document.querySelectorAll("[data-delete-request]").forEach((btn) => {
    btn.addEventListener("click", () => deleteRequest(Number(btn.dataset.deleteRequest)));
  });
}

function requestUserCanDownload(row) {
  if (ME.options.is_admin) {
    const perm = ME.permissions.find((item) => Number(item.user_id || 0) === Number(row.user_id || 0));
    return Number(perm?.can_download || 0) === 1;
  }
  return Boolean(ME.options.can_download);
}

function renderRequestStatus(row) {
  const status = String(row.status || "PENDING").toUpperCase();
  if (status === "APPROVED" && !requestUserCanDownload(row)) {
    return `
      <div class="me-actions">
        <span class="me-status old good">Approved</span>
        <span class="me-status bad">Blocked / Revoked</span>
      </div>
    `;
  }
  return `<span class="me-status ${statusClass(status)}">${esc(status)}</span>`;
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

async function editRequest(requestId) {
  const row = ME.requests.find((item) => Number(item.id) === requestId);
  const note = prompt("Update request note", row?.request_note || "");
  if (note === null) return;

  try {
    await api(`/api/marketing-emails/download-requests/${requestId}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ request_note: note })
    });
    await loadRequests();
    showMsg("requestMsg", "Request updated.", true);
  } catch (err) {
    showMsg("requestMsg", err.message, false);
  }
}

async function deleteRequest(requestId) {
  if (!confirm("Delete this download request record?")) return;
  try {
    await api(`/api/marketing-emails/download-requests/${requestId}`, { method: "DELETE" });
    await loadRequests();
    showMsg("requestMsg", "Request deleted.", true);
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
    body.innerHTML = `<tr><td colspan="4" class="me-muted-cell">No download logs found.</td></tr>`;
    setupTableScrollbars();
    return;
  }

  body.innerHTML = ME.logs.map((row) => `
    <tr>
      <td><b>${esc(row.file_name)}</b></td>
      <td>${esc(row.downloaded_by)}</td>
      <td>${esc(row.downloaded_at)}</td>
      <td><button class="btn danger mini" type="button" data-delete-log="${row.id}">Delete</button></td>
    </tr>
  `).join("");

  document.querySelectorAll("[data-delete-log]").forEach((btn) => {
    btn.addEventListener("click", () => deleteDownloadLog(Number(btn.dataset.deleteLog)));
  });

  setupTableScrollbars();
}

async function deleteDownloadLog(logId) {
  if (!confirm("Move this download log to trash?")) return;
  try {
    await api(`/api/marketing-emails/download-logs/${logId}/delete`, { method: "POST" });
    await loadDownloadLogs();
    await loadTrash();
    showMsg("requestMsg", "Download log moved to trash.", true);
  } catch (err) {
    showMsg("requestMsg", err.message, false);
  }
}

async function loadTrash() {
  if (!ME.options.is_admin) return;
  const out = await api("/api/marketing-emails/trash");
  ME.trash = out.data || { leads: [], exports: [], logs: [] };
  renderTrash();
}

function renderTrash() {
  renderTrashLeads();
  renderTrashExports();
  renderTrashLogs();
  setupTableScrollbars();
}

function renderTrashLeads() {
  const body = $("trashLeadBody");
  if (!body) return;
  const rows = ME.trash.leads || [];
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="7" class="me-muted-cell">No deleted leads.</td></tr>`;
    return;
  }

  body.innerHTML = rows.map((row) => `
    <tr>
      <td><b>${esc(row.company_name || "-")}</b></td>
      <td>${esc(row.email || "-")}</td>
      <td>${esc(row.category || "-")}</td>
      <td>${esc(row.country || "-")}</td>
      <td>${esc(row.deleted_by || "-")}</td>
      <td>${esc(row.deleted_at || "-")}</td>
      <td>
        <div class="me-actions">
          <button class="btn mini" type="button" data-recover-trash="lead:${row.id}">Recover</button>
          <button class="btn danger mini" type="button" data-permanent-trash="lead:${row.id}">Delete Permanently</button>
        </div>
      </td>
    </tr>
  `).join("");

  bindTrashActions();
}

function renderTrashExports() {
  const body = $("trashExportBody");
  if (!body) return;
  const rows = ME.trash.exports || [];
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="7" class="me-muted-cell">No deleted export files.</td></tr>`;
    return;
  }

  body.innerHTML = rows.map((row) => `
    <tr>
      <td><b>${esc(row.file_name || "-")}</b></td>
      <td>${esc(row.category || "-")}</td>
      <td>${esc(row.country || "-")}</td>
      <td>${esc(row.lead_count || 0)}</td>
      <td>${esc(row.deleted_by || "-")}</td>
      <td>${esc(row.deleted_at || "-")}</td>
      <td>
        <div class="me-actions">
          <button class="btn mini" type="button" data-recover-trash="export:${row.id}">Recover</button>
          <button class="btn danger mini" type="button" data-permanent-trash="export:${row.id}">Delete Permanently</button>
        </div>
      </td>
    </tr>
  `).join("");

  bindTrashActions();
}

function renderTrashLogs() {
  const body = $("trashLogBody");
  if (!body) return;
  const rows = ME.trash.logs || [];
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="6" class="me-muted-cell">No deleted download logs.</td></tr>`;
    return;
  }

  body.innerHTML = rows.map((row) => `
    <tr>
      <td><b>${esc(row.file_name || "-")}</b></td>
      <td>${esc(row.downloaded_by || "-")}</td>
      <td>${esc(row.downloaded_at || "-")}</td>
      <td>${esc(row.deleted_by || "-")}</td>
      <td>${esc(row.deleted_at || "-")}</td>
      <td>
        <div class="me-actions">
          <button class="btn mini" type="button" data-recover-trash="log:${row.id}">Recover</button>
          <button class="btn danger mini" type="button" data-permanent-trash="log:${row.id}">Delete Permanently</button>
        </div>
      </td>
    </tr>
  `).join("");

  bindTrashActions();
}

function bindTrashActions() {
  document.querySelectorAll("[data-recover-trash]").forEach((btn) => {
    btn.onclick = () => {
      const [type, id] = btn.dataset.recoverTrash.split(":");
      recoverTrashItem(type, Number(id));
    };
  });

  document.querySelectorAll("[data-permanent-trash]").forEach((btn) => {
    btn.onclick = () => {
      const [type, id] = btn.dataset.permanentTrash.split(":");
      permanentDeleteTrashItem(type, Number(id));
    };
  });
}

async function recoverTrashItem(type, id) {
  try {
    await api(`/api/marketing-emails/trash/${type}/${id}/recover`, { method: "POST" });
    await loadLeads();
    await loadExports();
    await loadTrash();
    showMsg("trashMsg", "Item recovered.", true);
  } catch (err) {
    showMsg("trashMsg", err.message, false);
  }
}

async function permanentDeleteTrashItem(type, id) {
  if (!confirm("Permanently delete this item? This cannot be undone.")) return;
  try {
    await api(`/api/marketing-emails/trash/${type}/${id}/permanent`, { method: "DELETE" });
    await loadTrash();
    showMsg("trashMsg", "Item permanently deleted.", true);
  } catch (err) {
    showMsg("trashMsg", err.message, false);
  }
}

function switchTab(tabName) {
  document.querySelectorAll(".me-tab").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.tab === tabName);
  });
  document.querySelectorAll(".me-section").forEach((section) => {
    section.classList.toggle("active", section.id === `tab-${tabName}`);
  });
  setupTableScrollbars();
}

function setupTableScrollbars() {
  document.querySelectorAll(".me-x-scroll").forEach((scroll) => scroll.remove());
}

function setupLeadFormToggle() {
  const page = document.querySelector(".me-page");
  const btn = $("leadFormToggle");
  if (!page || !btn) return;

  const sync = () => {
    const collapsed = page.classList.contains("form-collapsed");
    btn.textContent = "";
    btn.classList.toggle("is-collapsed", collapsed);
    btn.setAttribute("aria-label", collapsed ? "Expand lead form" : "Collapse lead form");
  };

  btn.addEventListener("click", () => {
    page.classList.toggle("form-collapsed");
    sync();
    window.setTimeout(setupTableScrollbars, 220);
  });

  sync();
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
    await loadRequests();
    await loadPermissions();
    await loadDownloadLogs();
    await loadTrash();
    setupTableScrollbars();
  } catch (err) {
    showMsg("leadTableMsg", err.message, false);
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  setupLeadFormToggle();

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
  $("applyExportFiltersBtn")?.addEventListener("click", loadExports);
  $("refreshAllBtn")?.addEventListener("click", refreshAll);
  $("selectAllLeadsBtn")?.addEventListener("click", selectAllVisibleLeads);
  $("clearLeadSelectionBtn")?.addEventListener("click", clearLeadSelection);
  $("leadSelectAllBox")?.addEventListener("change", (event) => {
    if (event.target.checked) {
      selectAllVisibleLeads();
    } else {
      clearLeadSelection();
    }
  });
  $("bulkVerifyBtn")?.addEventListener("click", bulkVerify);
  $("createExportBtn")?.addEventListener("click", createExport);
  $("updateExportBtn")?.addEventListener("click", updateExport);
  $("clearExportBtn")?.addEventListener("click", clearExportForm);
  $("exportStatus")?.addEventListener("change", updateSendDateVisibility);
  $("sendRound")?.addEventListener("input", updateSendDateVisibility);
  $("exportSelectAllBox")?.addEventListener("change", (event) => {
    if (event.target.checked) {
      selectAllVisibleExports();
    } else {
      clearExportSelection();
    }
  });
  $("requestDownloadBtn")?.addEventListener("click", requestDownloadAccess);
  $("searchInput")?.addEventListener("input", scheduleSearchReload);
  $("searchInput")?.addEventListener("keydown", async (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      await loadLeads();
    }
  });
  ["employeeFilter", "categoryFilter", "countryFilter", "statusFilter", "callStatusFilter"].forEach((id) => {
    $(id)?.addEventListener("change", async () => {
      await loadLeads();
      await loadExports();
    });
  });

  $("exportStatusFilter")?.addEventListener("change", loadExports);

  window.addEventListener("resize", setupTableScrollbars);

  clearLeadForm();
  await loadOptions();
  clearExportForm();
  await refreshAll();
});
