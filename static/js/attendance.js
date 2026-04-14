const $ = (id) => document.getElementById(id);

let ME = null;
let USERS = [];
let MY_ROWS = [];
let ADMIN_ROWS = [];

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

function showMsg(id, text, ok = true) {
  const el = $(id);
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

function localTodayParts() {
  const now = new Date();
  return {
    year: now.getFullYear(),
    month: String(now.getMonth() + 1).padStart(2, "0"),
    day: String(now.getDate()).padStart(2, "0")
  };
}

function monthNow() {
  const p = localTodayParts();
  return `${p.year}-${p.month}`;
}

function prettySchedule(row) {
  if (Number(row.scheduled_workday || 0) !== 1) {
    return "Not workday";
  }
  const s = (row.schedule_start || "").trim();
  const e = (row.schedule_end || "").trim();
  if (s && e) return `${esc(s)} to ${esc(e)}`;
  if (s) return esc(s);
  return "Workday";
}

function statusBadge(status) {
  const s = String(status || "UNMARKED").toUpperCase();
  let cls = "unmarked";
  if (s === "PRESENT") cls = "present";
  if (s === "ABSENT") cls = "absent";
  return `<span class="att-status ${cls}">${esc(s)}</span>`;
}

function confirmBadge(row) {
  const confirmed = Number(row.admin_confirmed || 0) === 1;
  return confirmed
    ? `<span class="att-status confirmed">CONFIRMED</span>`
    : `<span class="att-status pending">PENDING</span>`;
}

function isLocked(row) {
  return Number(row.admin_confirmed || 0) === 1;
}

async function loadMe() {
  ME = await api("/api/me");
  if ((ME.role || "").toUpperCase() === "ADMIN") {
    $("adminPanel")?.classList.add("show");
  }
}

async function loadUsers() {
  if ((ME.role || "").toUpperCase() !== "ADMIN") return;
  const out = await api("/api/attendance/users");
  USERS = out.data || [];

  const sel = $("adminUserSelect");
  if (!sel) return;

  sel.innerHTML = `<option value="">Select user</option>` + USERS.map((u) => {
    return `<option value="${u.id}">${esc(u.full_name || u.username)} (${esc(u.role)})</option>`;
  }).join("");
}

function renderMyRows() {
  const body = $("myTbody");
  if (!body) return;

  if (!MY_ROWS.length) {
    body.innerHTML = `<tr><td colspan="6">No data</td></tr>`;
    return;
  }

  body.innerHTML = MY_ROWS.map((row, i) => {
    const locked = isLocked(row);
    return `
      <tr>
        <td class="att-date">${esc(row.attendance_date)}</td>
        <td class="att-schedule">${prettySchedule(row)}</td>
        <td>
          <select data-my-status="${i}" ${locked ? "disabled" : ""}>
            <option value="UNMARKED" ${row.marked_status === "UNMARKED" ? "selected" : ""}>Unmarked</option>
            <option value="PRESENT" ${row.marked_status === "PRESENT" ? "selected" : ""}>Present</option>
            <option value="ABSENT" ${row.marked_status === "ABSENT" ? "selected" : ""}>Absent</option>
          </select>
        </td>
        <td>${confirmBadge(row)}</td>
        <td>
          <input
            type="text"
            data-my-note="${i}"
            value="${esc(row.employee_note || "")}"
            placeholder="Optional note"
            ${locked ? "disabled" : ""}
          />
          ${locked ? `<div class="att-note">Locked after admin confirmation</div>` : ""}
        </td>
        <td>
          <div class="att-actions">
            <button class="btn mini" type="button" data-my-save="${i}" ${locked ? "disabled" : ""}>Save</button>
          </div>
        </td>
      </tr>
    `;
  }).join("");

  document.querySelectorAll("[data-my-status]").forEach((el) => {
    el.addEventListener("change", () => {
      const i = Number(el.dataset.myStatus);
      if (!MY_ROWS[i]) return;
      MY_ROWS[i].marked_status = el.value;
    });
  });

  document.querySelectorAll("[data-my-note]").forEach((el) => {
    el.addEventListener("input", () => {
      const i = Number(el.dataset.myNote);
      if (!MY_ROWS[i]) return;
      MY_ROWS[i].employee_note = el.value;
    });
  });

  document.querySelectorAll("[data-my-save]").forEach((btn) => {
    btn.onclick = async () => {
      const i = Number(btn.dataset.mySave);
      const row = MY_ROWS[i];
      try {
        await api("/api/attendance/my/save", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            attendance_date: row.attendance_date,
            marked_status: row.marked_status,
            employee_note: row.employee_note || ""
          })
        });
        showMsg("myMsg", "Attendance saved.", true);
        await loadMyRows();
      } catch (err) {
        showMsg("myMsg", err.message, false);
      }
    };
  });
}

function renderAdminRows() {
  const body = $("adminTbody");
  if (!body) return;

  if (!ADMIN_ROWS.length) {
    body.innerHTML = `<tr><td colspan="6">Select a user first</td></tr>`;
    return;
  }

  body.innerHTML = ADMIN_ROWS.map((row, i) => {
    return `
      <tr>
        <td class="att-date">${esc(row.attendance_date)}</td>
        <td class="att-schedule">${prettySchedule(row)}</td>
        <td>${statusBadge(row.marked_status)}</td>
        <td>${confirmBadge(row)}</td>
        <td>${esc(row.employee_note || "")}</td>
        <td>
          <div class="att-actions">
            <button class="btn mini" type="button" data-admin-action="CONFIRM_PRESENT" data-i="${i}">Confirm Present</button>
            <button class="btn ghost mini" type="button" data-admin-action="CONFIRM_ABSENT" data-i="${i}">Confirm Absent</button>
            <button class="btn ghost mini" type="button" data-admin-action="UNCONFIRM" data-i="${i}">Unconfirm</button>
            <button class="btn ghost mini" type="button" data-admin-action="DELETE" data-i="${i}">Delete</button>
          </div>
        </td>
      </tr>
    `;
  }).join("");

  document.querySelectorAll("[data-admin-action]").forEach((btn) => {
    btn.onclick = async () => {
      const i = Number(btn.dataset.i);
      const action = btn.dataset.adminAction;
      const row = ADMIN_ROWS[i];
      const userId = $("adminUserSelect")?.value || "";

      if (!userId) {
        showMsg("adminMsg", "Select a user first.", false);
        return;
      }

      if (action === "DELETE") {
        const yes = confirm("Delete this attendance row?");
        if (!yes) return;
      }

      try {
        await api("/api/attendance/admin/action", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            user_id: Number(userId),
            attendance_date: row.attendance_date,
            action,
            employee_note: row.employee_note || ""
          })
        });

        showMsg("adminMsg", "Attendance updated.", true);
        await loadAdminRows();
      } catch (err) {
        showMsg("adminMsg", err.message, false);
      }
    };
  });
}

async function loadMyRows() {
  const month = $("myMonth")?.value || monthNow();
  const out = await api(`/api/attendance/my?month=${encodeURIComponent(month)}`);
  MY_ROWS = out.data || [];
  renderMyRows();
}

async function loadAdminRows() {
  if ((ME.role || "").toUpperCase() !== "ADMIN") return;

  const userId = $("adminUserSelect")?.value || "";
  const month = $("adminMonth")?.value || monthNow();

  if (!userId) {
    ADMIN_ROWS = [];
    renderAdminRows();
    return;
  }

  const out = await api(`/api/attendance/admin/month?user_id=${encodeURIComponent(userId)}&month=${encodeURIComponent(month)}`);
  ADMIN_ROWS = out.data || [];
  renderAdminRows();
}

document.addEventListener("DOMContentLoaded", async () => {
  if ($("myMonth")) $("myMonth").value = monthNow();
  if ($("adminMonth")) $("adminMonth").value = monthNow();

  await loadMe();
  await loadUsers();
  await loadMyRows();
  await loadAdminRows();

  $("loadMyBtn")?.addEventListener("click", loadMyRows);
  $("loadAdminBtn")?.addEventListener("click", loadAdminRows);
  $("adminUserSelect")?.addEventListener("change", loadAdminRows);
});
