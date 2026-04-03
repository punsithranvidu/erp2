const $ = (id) => document.getElementById(id);

let ME = null;
let WS_ROWS = [];
let USERS = [];
let CURRENT_MODE = "self"; // self | admin-view
let CURRENT_TARGET_USER_ID = null;
let CURRENT_TARGET_ROLE = "";
let CURRENT_TARGET_NAME = "";

async function safeJson(res) {
  try {
    return await res.json();
  } catch {
    return {};
  }
}

async function api(url, opts = {}) {
  const res = await fetch(url, {
    credentials: "same-origin",
    ...opts
  });
  const data = await safeJson(res);
  if (!res.ok) throw new Error(data.error || "Request failed");
  return data;
}

function showMsg(text, ok = true) {
  const el = $("wsMsg");
  if (!el) return;
  el.textContent = text || "";
  el.className = "msg " + (ok ? "ok" : "bad");
}

function esc(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function monthNow() {
  return new Date().toISOString().slice(0, 7);
}

function isAdmin() {
  return String(ME?.role || "").toUpperCase() === "ADMIN";
}

function isAdminViewingOtherUser() {
  return isAdmin() && CURRENT_MODE === "admin-view" && !!CURRENT_TARGET_USER_ID;
}

function currentTableIsEditableByCurrentUser(row) {
  if (isAdminViewingOtherUser()) return false;

  const status = String(row.status || "DRAFT").toUpperCase();
  return !["SUBMITTED", "APPROVED", "REOPEN_REQUESTED"].includes(status);
}

function statusBadge(status) {
  const s = String(status || "DRAFT").toUpperCase();
  const cls =
    s === "APPROVED" ? "approved" :
    s === "SUBMITTED" ? "submitted" :
    s === "RETURNED" ? "returned" :
    s === "REOPEN_REQUESTED" ? "reopen_requested" : "draft";

  return `<span class="ws-status ${cls}">${esc(s)}</span>`;
}

function scheduleBadge(row) {
  const scheduled = Number(row.scheduled_workday || 0) === 1;
  if (!scheduled) {
    return `<span class="ws-schedule-badge off">Off Day</span>`;
  }

  const start = String(row.schedule_start || "").trim();
  const end = String(row.schedule_end || "").trim();
  const timeText = start && end ? `${start} - ${end}` : (start || end || "Work day");

  return `
    <div>
      <span class="ws-schedule-badge work">Work Day</span>
      <div class="ws-small" style="margin-top:6px;">${esc(timeText)}</div>
    </div>
  `;
}

function workdaySelectHtml(row, i, locked) {
  return `
    <select data-k="is_workday" data-i="${i}" ${locked ? "disabled" : ""}>
      <option value="1" ${Number(row.is_workday || 0) === 1 ? "selected" : ""}>Workday</option>
      <option value="0" ${Number(row.is_workday || 0) === 0 ? "selected" : ""}>Not Workday</option>
    </select>
  `;
}

function getCurrentViewLabel() {
  const badge = $("wsCurrentView");
  if (!badge) return;

  if (!isAdmin()) {
    badge.textContent = "My Worksheet";
    return;
  }

  if (CURRENT_MODE === "self") {
    badge.textContent = "My Worksheet";
    return;
  }

  badge.textContent = CURRENT_TARGET_NAME
    ? `${CURRENT_TARGET_NAME} Worksheet`
    : "Selected User Worksheet";
}

function toggleAdminEmptyState(show) {
  const empty = $("wsAdminEmptyState");
  const wrap = $("wsTableWrap");

  if (empty) empty.style.display = show ? "" : "none";
  if (wrap) wrap.style.display = show ? "none" : "";
}

function updateAdminModeButtons() {
  const myBtn = $("wsMyTableBtn");
  if (!myBtn) return;

  if (CURRENT_MODE === "self") {
    myBtn.textContent = "My Table";
  } else {
    myBtn.textContent = "Back to My Table";
  }
}

function rowClass(row) {
  return Number(row.is_workday || 0) === 0 ? "ws-day-off" : "";
}

function renderRows() {
  const body = $("wsBody");
  if (!body) return;

  getCurrentViewLabel();
  updateAdminModeButtons();

  if (isAdmin() && CURRENT_MODE === "admin-view" && !CURRENT_TARGET_USER_ID) {
    body.innerHTML = "";
    toggleAdminEmptyState(true);
    return;
  }

  toggleAdminEmptyState(false);

  if (!WS_ROWS.length) {
    body.innerHTML = `<tr><td colspan="7" class="ws-small">No data</td></tr>`;
    return;
  }

  body.innerHTML = WS_ROWS.map((row, i) => {
    const locked = !currentTableIsEditableByCurrentUser(row);
    const rowLockedClass = locked ? "ws-locked" : "";
    const adminView = isAdminViewingOtherUser();

    return `
      <tr class="${rowClass(row)}">
        <td>
          <b>${esc(row.work_date)}</b>
        </td>

        <td>
          ${scheduleBadge(row)}
        </td>

        <td>
          ${adminView
            ? `<div class="ws-small"><b>${Number(row.is_workday || 0) === 1 ? "Workday" : "Not Workday"}</b></div>`
            : workdaySelectHtml(row, i, locked)}
        </td>

        <td>
          <textarea class="${rowLockedClass}" data-k="summary" data-i="${i}" ${locked ? "disabled" : ""}>${esc(row.summary || "")}</textarea>
        </td>

        <td>
          ${statusBadge(row.status)}
          ${row.reopen_reason ? `<div class="ws-small" style="margin-top:6px;">Reopen reason: ${esc(row.reopen_reason)}</div>` : ""}
        </td>

        <td>
          ${adminView
            ? `<textarea data-admin-comment="${i}">${esc(row.admin_comment || "")}</textarea>`
            : `<div class="ws-small">${esc(row.admin_comment || "") || "-"}</div>`}
        </td>

        <td>
          ${adminView ? renderAdminActions(row, i) : renderSelfActions(row, i)}
        </td>
      </tr>
    `;
  }).join("");

  bindInputs();
  bindActions();
}

function renderSelfActions(row, i) {
  const status = String(row.status || "DRAFT").toUpperCase();
  const editable = currentTableIsEditableByCurrentUser(row);

  let html = `<div class="ws-actions">`;

  if (editable) {
    html += `<button class="btn mini" data-save="${i}" type="button">Save</button>`;
    html += `<button class="btn ghost mini" data-submit="${i}" type="button">Submit</button>`;
  }

  if (!editable && ["SUBMITTED", "APPROVED", "REOPEN_REQUESTED"].includes(status)) {
    html += `<button class="btn ghost mini" data-reopen="${i}" type="button">Request Reopen</button>`;
  }

  html += `</div>`;
  return html;
}

function renderAdminActions(row, i) {
  if (!row.id) {
    return `<div class="ws-small">No record saved yet</div>`;
  }

  const status = String(row.status || "DRAFT").toUpperCase();

  let html = `<div class="ws-actions">`;

  html += `<button class="btn mini" data-admin-approve="${i}" type="button">Approve</button>`;
  html += `<button class="btn ghost mini" data-admin-reopen="${i}" type="button">Reopen</button>`;
  html += `<button class="btn ghost mini" data-admin-return="${i}" type="button">Return</button>`;
  html += `<button class="btn ghost mini" data-admin-delete="${i}" type="button">Delete</button>`;

  if (status === "APPROVED") {
    html += `<div class="ws-small" style="width:100%;">Approved row</div>`;
  }

  html += `</div>`;
  return html;
}

function bindInputs() {
  if (isAdminViewingOtherUser()) {
    document.querySelectorAll("[data-admin-comment]").forEach((el) => {
      el.addEventListener("input", () => {
        const i = Number(el.dataset.adminComment);
        if (!WS_ROWS[i]) return;
        WS_ROWS[i].admin_comment = el.value;
      });
    });
    return;
  }

  document.querySelectorAll("[data-k]").forEach((el) => {
    el.addEventListener("input", () => {
      const i = Number(el.dataset.i);
      const k = el.dataset.k;
      if (!WS_ROWS[i]) return;

      if (k === "is_workday") {
        WS_ROWS[i][k] = Number(el.value || 0);
      } else {
        WS_ROWS[i][k] = el.value;
      }
    });

    el.addEventListener("change", () => {
      const i = Number(el.dataset.i);
      const k = el.dataset.k;
      if (!WS_ROWS[i]) return;

      if (k === "is_workday") {
        WS_ROWS[i][k] = Number(el.value || 0);
        renderRows();
      }
    });
  });
}

async function saveRow(i) {
  const row = WS_ROWS[i];
  await api("/api/worksheet/my/save", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      work_date: row.work_date,
      is_workday: Number(row.is_workday || 0),
      summary: row.summary || ""
    })
  });
}

async function submitRow(i) {
  const row = WS_ROWS[i];
  await api("/api/worksheet/my/submit", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      work_date: row.work_date,
      is_workday: Number(row.is_workday || 0),
      summary: row.summary || ""
    })
  });
}

async function requestReopen(i) {
  const row = WS_ROWS[i];
  const reason = prompt("Why do you want admin to reopen this row?") || "";

  await api("/api/worksheet/my/request-reopen", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      work_date: row.work_date,
      reason
    })
  });
}

async function adminAction(i, action) {
  const row = WS_ROWS[i];

  if (!row?.id) {
    throw new Error("Worksheet row not found");
  }

  let confirmText = "";
  if (action === "APPROVE") confirmText = "Approve this row?";
  if (action === "REOPEN") confirmText = "Reopen this row for editing?";
  if (action === "RETURN") confirmText = "Return this row to the user?";
  if (action === "DELETE") confirmText = "Delete this worksheet row?";

  if (confirmText && !confirm(confirmText)) return;

  await api("/api/worksheet/admin/action", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      entry_id: row.id,
      action,
      admin_comment: row.admin_comment || ""
    })
  });
}

function bindActions() {
  if (isAdminViewingOtherUser()) {
    document.querySelectorAll("[data-admin-approve]").forEach((btn) => {
      btn.onclick = async () => {
        try {
          await adminAction(Number(btn.dataset.adminApprove), "APPROVE");
          await loadAdminSelectedWorksheet();
          showMsg("Row approved.", true);
        } catch (err) {
          showMsg(err.message, false);
        }
      };
    });

    document.querySelectorAll("[data-admin-reopen]").forEach((btn) => {
      btn.onclick = async () => {
        try {
          await adminAction(Number(btn.dataset.adminReopen), "REOPEN");
          await loadAdminSelectedWorksheet();
          showMsg("Row reopened.", true);
        } catch (err) {
          showMsg(err.message, false);
        }
      };
    });

    document.querySelectorAll("[data-admin-return]").forEach((btn) => {
      btn.onclick = async () => {
        try {
          await adminAction(Number(btn.dataset.adminReturn), "RETURN");
          await loadAdminSelectedWorksheet();
          showMsg("Row returned.", true);
        } catch (err) {
          showMsg(err.message, false);
        }
      };
    });

    document.querySelectorAll("[data-admin-delete]").forEach((btn) => {
      btn.onclick = async () => {
        try {
          await adminAction(Number(btn.dataset.adminDelete), "DELETE");
          await loadAdminSelectedWorksheet();
          showMsg("Row deleted.", true);
        } catch (err) {
          showMsg(err.message, false);
        }
      };
    });

    return;
  }

  document.querySelectorAll("[data-save]").forEach((btn) => {
    btn.onclick = async () => {
      try {
        await saveRow(Number(btn.dataset.save));
        await loadMyWorksheet();
        showMsg("Draft saved.", true);
      } catch (err) {
        showMsg(err.message, false);
      }
    };
  });

  document.querySelectorAll("[data-submit]").forEach((btn) => {
    btn.onclick = async () => {
      try {
        await submitRow(Number(btn.dataset.submit));
        await loadMyWorksheet();
        showMsg("Submitted to admin.", true);
      } catch (err) {
        showMsg(err.message, false);
      }
    };
  });

  document.querySelectorAll("[data-reopen]").forEach((btn) => {
    btn.onclick = async () => {
      try {
        await requestReopen(Number(btn.dataset.reopen));
        await loadMyWorksheet();
        showMsg("Reopen request sent.", true);
      } catch (err) {
        showMsg(err.message, false);
      }
    };
  });
}

async function loadMe() {
  ME = await api("/api/me");
}

async function loadUsersForAdmin() {
  if (!isAdmin()) return;

  const out = await api("/api/worksheet/users");
  USERS = out.data || [];

  const sel = $("wsUserSelect");
  if (!sel) return;

  sel.innerHTML = `<option value="">-- Select user to view --</option>` + USERS.map((u) => {
    const role = String(u.role || "").toUpperCase();
    return `<option value="${u.id}" data-role="${esc(role)}">${esc(u.full_name || u.username)} (${esc(role)})</option>`;
  }).join("");
}

async function loadMyWorksheet() {
  CURRENT_MODE = "self";
  CURRENT_TARGET_USER_ID = null;
  CURRENT_TARGET_ROLE = String(ME?.role || "").toUpperCase();
  CURRENT_TARGET_NAME = "My Worksheet";

  const month = $("wsMonth")?.value || monthNow();
  const out = await api(`/api/worksheet/my?month=${month}`);
  WS_ROWS = out.data || [];

  renderRows();
}

async function loadAdminSelectedWorksheet() {
  const sel = $("wsUserSelect");
  const month = $("wsMonth")?.value || monthNow();

  if (!sel || !sel.value) {
    CURRENT_MODE = "admin-view";
    CURRENT_TARGET_USER_ID = null;
    CURRENT_TARGET_ROLE = "";
    CURRENT_TARGET_NAME = "";
    WS_ROWS = [];
    renderRows();
    return;
  }

  const selectedOption = sel.options[sel.selectedIndex];
  CURRENT_MODE = "admin-view";
  CURRENT_TARGET_USER_ID = Number(sel.value);
  CURRENT_TARGET_ROLE = String(selectedOption?.dataset?.role || "").toUpperCase();
  CURRENT_TARGET_NAME = selectedOption ? selectedOption.textContent : "Selected User";

  const out = await api(`/api/worksheet/admin/month?user_id=${CURRENT_TARGET_USER_ID}&month=${month}`);
  WS_ROWS = out.data || [];
  renderRows();
}

document.addEventListener("DOMContentLoaded", async () => {
  try {
    if ($("wsMonth")) {
      $("wsMonth").value = monthNow();
    }

    await loadMe();

    if (isAdmin()) {
      await loadUsersForAdmin();
      CURRENT_MODE = "admin-view";
      CURRENT_TARGET_USER_ID = null;
      CURRENT_TARGET_ROLE = "";
      CURRENT_TARGET_NAME = "";
      renderRows();
    } else {
      await loadMyWorksheet();
    }

    $("wsLoadBtn")?.addEventListener("click", async () => {
      try {
        if (isAdmin()) {
          if ($("wsUserSelect")?.value) {
            await loadAdminSelectedWorksheet();
          } else {
            await loadMyWorksheet();
          }
        } else {
          await loadMyWorksheet();
        }
      } catch (err) {
        showMsg(err.message, false);
      }
    });

    $("wsMyTableBtn")?.addEventListener("click", async () => {
      try {
        if ($("wsUserSelect")) $("wsUserSelect").value = "";
        await loadMyWorksheet();
        showMsg("Showing your own worksheet table.", true);
      } catch (err) {
        showMsg(err.message, false);
      }
    });

    $("wsUserSelect")?.addEventListener("change", async () => {
      try {
        if ($("wsUserSelect").value) {
          await loadAdminSelectedWorksheet();
        } else {
          renderRows();
        }
      } catch (err) {
        showMsg(err.message, false);
      }
    });
  } catch (err) {
    showMsg(err.message || "Failed to load worksheet.", false);
  }
});