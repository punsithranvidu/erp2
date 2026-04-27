const $ = (id) => document.getElementById(id);

let ME = null;
let USERS = [];
let MONTH_DATA = [];
let REQUESTS = [];
let EDIT_EVENT_ID = null;
let EDIT_HOLIDAY_ID = null;
let SHOWN_REMINDERS = new Set();

function localDateParts() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  return {
    month: `${year}-${month}`,
    date: `${year}-${month}-${day}`
  };
}

const LOCAL_NOW = localDateParts();
let CURRENT_MONTH = LOCAL_NOW.month;
let SELECTED_DATE = LOCAL_NOW.date;

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

function scope() {
  return document.body.dataset.calendarScope || "SHARED";
}

function monthLabel(ym) {
  const [y, m] = ym.split("-").map(Number);
  return new Date(y, m - 1, 1).toLocaleString([], {
    month: "long",
    year: "numeric"
  });
}

function weekNames() {
  return ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
}

function getCurrentUserId() {
  const me = USERS.find((x) => x.username === ME.user);
  return me ? Number(me.id) : null;
}

function getViewValue() {
  return $("viewUserSelect")?.value || "active";
}

function isActiveView() {
  return getViewValue() === "active";
}

function isMyView() {
  return getViewValue() === "my";
}

function isUserView() {
  return getViewValue().startsWith("user:");
}

function selectedUserId() {
  const val = getViewValue();
  const meId = getCurrentUserId();

  if (val === "my") return Number(meId || 0);
  if (val === "active") return Number(meId || 0);
  if (val.startsWith("user:")) {
    return Number(val.split(":")[1] || meId || 0);
  }

  return Number(meId || 0);
}

function getSelectedCalendarLabel() {
  const val = getViewValue();

  if (val === "active") return "Active Calendar";
  if (val === "my") return "My Calendar";

  if (val.startsWith("user:")) {
    const uid = Number(val.split(":")[1]);
    const user = USERS.find((u) => Number(u.id) === uid);
    if (user) return `${user.full_name || user.username} Calendar`;
  }

  return "Calendar";
}

function updateViewModeNote() {
  const el = $("viewModeNote");
  if (!el) return;

  if (isActiveView()) {
    el.textContent = "Active calendar view. Shared approved items are shown here.";
  } else if (isMyView()) {
    el.textContent = "My personal calendar view.";
  } else {
    el.textContent = `${getSelectedCalendarLabel()}.`;
  }
}

function updateHistoryMonthNote() {
  const el = $("historyMonthNote");
  if (!el) return;
  el.textContent = `Showing ${monthLabel(CURRENT_MONTH)} requests / history only`;
}

function updatePanelsByView() {
  const calendarActionSelect = $("calendarActionSelect");
  const scheduleInfo = $("scheduleViewInfo");
  const scheduleEditor = $("scheduleEditor");
  const saveScheduleBtn = $("saveScheduleBtn");
  const panelSchedule = $("panelSchedule");

  if (!calendarActionSelect) return;

  if (ME.role !== "ADMIN") {
    if (panelSchedule) panelSchedule.classList.remove("open");
    return;
  }

  if (isActiveView()) {
    if (scheduleInfo) {
      scheduleInfo.textContent = "Work schedule cannot be edited in Active Calendar view.";
    }
    if (scheduleEditor) {
      scheduleEditor.classList.add("cal-hidden");
    }
    if (saveScheduleBtn) {
      saveScheduleBtn.classList.add("cal-hidden");
    }

    if (calendarActionSelect.value === "schedule") {
      openActionPanel("unavailability");
    }
  } else {
    if (scheduleInfo) {
      scheduleInfo.textContent = "Admin can manage work schedule here.";
    }
    if (scheduleEditor) {
      scheduleEditor.classList.remove("cal-hidden");
    }
    if (saveScheduleBtn) {
      saveScheduleBtn.classList.remove("cal-hidden");
    }
  }
}

function formatTimeRange(start, end, allDay) {
  if (Number(allDay) === 1) return "All day";
  const s = (start || "").trim();
  const e = (end || "").trim();

  if (s && e) return `${esc(s)} - ${esc(e)}`;
  if (s) return esc(s);
  return "Time not set";
}

function prettyStatus(status) {
  const s = String(status || "").toUpperCase();
  if (s === "CANCEL_REQUESTED") return "CANCEL REQUESTED";
  return s || "UNKNOWN";
}

function openActionPanel(name) {
  const names = ["schedule", "unavailability", "event", "holiday"];

  names.forEach((key) => {
    const panel =
      key === "schedule"
        ? $("panelSchedule")
        : key === "unavailability"
          ? $("panelUnavailability")
          : key === "event"
            ? $("panelEvent")
            : $("panelHoliday");

    if (!panel) return;

    if (key === name) {
      panel.classList.add("open");
    } else {
      panel.classList.remove("open");
    }
  });

  if ($("calendarActionSelect")) {
    $("calendarActionSelect").value = name;
  }
}

function toggleAudienceUsers() {
  const wrap = $("audienceUsersWrap");
  const type = $("audienceType")?.value || "ONLY_ME";
  if (!wrap) return;

  if (type === "SELECTED_USERS") {
    wrap.classList.add("show");
  } else {
    wrap.classList.remove("show");
    document.querySelectorAll("[data-aud-user]").forEach((el) => {
      el.checked = false;
    });
  }
}

function clearEventForm() {
  EDIT_EVENT_ID = null;

  if ($("evTitle")) $("evTitle").value = "";
  if ($("evDate")) $("evDate").value = SELECTED_DATE || "";
  if ($("evAllDay")) $("evAllDay").checked = true;
  if ($("evStart")) $("evStart").value = "";
  if ($("evEnd")) $("evEnd").value = "";
  if ($("mainReminderTime")) $("mainReminderTime").value = "";
  if ($("rem1Date")) $("rem1Date").value = "";
  if ($("rem1Time")) $("rem1Time").value = "";
  if ($("rem2Date")) $("rem2Date").value = "";
  if ($("rem2Time")) $("rem2Time").value = "";
  if ($("rem3Date")) $("rem3Date").value = "";
  if ($("rem3Time")) $("rem3Time").value = "";
  if ($("evNotes")) $("evNotes").value = "";
  if ($("audienceType")) $("audienceType").value = "ONLY_ME";
  if ($("evShowInActive")) $("evShowInActive").checked = false;

  document.querySelectorAll("[data-aud-user]").forEach((el) => {
    el.checked = false;
  });

  toggleAudienceUsers();

  const btn = $("saveEventBtn");
  if (btn) btn.textContent = "Save Event";

  showMsg("eventMsg", "", true);
}

function clearHolidayForm() {
  EDIT_HOLIDAY_ID = null;

  if ($("holidayTitle")) $("holidayTitle").value = "";
  if ($("holidayDate")) $("holidayDate").value = SELECTED_DATE || "";
  if ($("holidayType")) $("holidayType").value = "GOVERNMENT_HOLIDAY";
  if ($("holidayNotes")) $("holidayNotes").value = "";

  const saveBtn = $("saveHolidayBtn");
  const updateBtn = $("updateHolidayBtn");
  const deleteBtn = $("deleteHolidayBtn");

  if (saveBtn) saveBtn.disabled = false;
  if (updateBtn) updateBtn.disabled = true;
  if (deleteBtn) deleteBtn.disabled = true;

  showMsg("holidayMsg", "", true);
}

function fillHolidayFormFromRow(h) {
  EDIT_HOLIDAY_ID = Number(h.id);

  if ($("holidayTitle")) $("holidayTitle").value = h.title || "";
  if ($("holidayDate")) $("holidayDate").value = h.holiday_date || "";
  if ($("holidayType")) $("holidayType").value = h.holiday_type || "GOVERNMENT_HOLIDAY";
  if ($("holidayNotes")) $("holidayNotes").value = h.notes || "";

  const saveBtn = $("saveHolidayBtn");
  const updateBtn = $("updateHolidayBtn");
  const deleteBtn = $("deleteHolidayBtn");

  if (saveBtn) saveBtn.disabled = true;
  if (updateBtn) updateBtn.disabled = false;
  if (deleteBtn) deleteBtn.disabled = false;

  openActionPanel("holiday");
  showMsg("holidayMsg", "Holiday loaded for editing.", true);
}

function fillEventFormFromRow(e) {
  EDIT_EVENT_ID = Number(e.id);

  if ($("evTitle")) $("evTitle").value = e.title || "";
  if ($("evDate")) $("evDate").value = e.event_date || "";
  if ($("evAllDay")) $("evAllDay").checked = Number(e.all_day || 0) === 1;
  if ($("evStart")) $("evStart").value = e.start_time || "";
  if ($("evEnd")) $("evEnd").value = e.end_time || "";
  if ($("evNotes")) $("evNotes").value = e.notes || "";
  if ($("audienceType")) $("audienceType").value = e.audience_type || "ONLY_ME";
  if ($("evShowInActive")) $("evShowInActive").checked = Number(e.show_in_active || 0) === 1;

  document.querySelectorAll("[data-aud-user]").forEach((el) => {
    el.checked = false;
  });

  const ids = String(e.audience_user_ids || "")
    .split(",")
    .map((x) => Number(x))
    .filter((x) => Number.isFinite(x));

  ids.forEach((id) => {
    const el = document.querySelector(`[data-aud-user="${id}"]`);
    if (el) el.checked = true;
  });

  if ($("mainReminderTime")) $("mainReminderTime").value = "";
  if ($("rem1Date")) $("rem1Date").value = "";
  if ($("rem1Time")) $("rem1Time").value = "";
  if ($("rem2Date")) $("rem2Date").value = "";
  if ($("rem2Time")) $("rem2Time").value = "";
  if ($("rem3Date")) $("rem3Date").value = "";
  if ($("rem3Time")) $("rem3Time").value = "";

  const reminderTimes = Array.isArray(e.reminder_times) ? e.reminder_times : [];

  if (reminderTimes[0]) {
    const v = String(reminderTimes[0]).replace("T", " ");
    const parts = v.split(" ");
    if (parts[1] && $("mainReminderTime")) {
      $("mainReminderTime").value = parts[1].slice(0, 5);
    }
  }

  const early = reminderTimes.slice(1, 4);
  early.forEach((item, idx) => {
    const v = String(item).replace("T", " ");
    const parts = v.split(" ");
    const d = parts[0] || "";
    const t = (parts[1] || "").slice(0, 5);

    if (idx === 0) {
      if ($("rem1Date")) $("rem1Date").value = d;
      if ($("rem1Time")) $("rem1Time").value = t;
    }
    if (idx === 1) {
      if ($("rem2Date")) $("rem2Date").value = d;
      if ($("rem2Time")) $("rem2Time").value = t;
    }
    if (idx === 2) {
      if ($("rem3Date")) $("rem3Date").value = d;
      if ($("rem3Time")) $("rem3Time").value = t;
    }
  });

  toggleAudienceUsers();
  openActionPanel("event");

  const btn = $("saveEventBtn");
  if (btn) btn.textContent = "Update Event";

  showMsg("eventMsg", "Event loaded for editing.", true);
}

async function ensureNotificationPermission() {
  if (!("Notification" in window)) return false;

  if (Notification.permission === "granted") return true;

  if (Notification.permission !== "denied") {
    const result = await Notification.requestPermission();
    return result === "granted";
  }

  return false;
}

async function loadMe() {
  ME = await api("/api/me");
}

async function loadUsers() {
  const out = await api("/api/calendar/users");
  USERS = out.data || [];
  renderUsers();
  renderAudienceUsers();
}

function renderUsers() {
  const sel = $("viewUserSelect");
  if (!sel) return;

  let html = "";
  html += `<option value="active">Active Calendar</option>`;
  html += `<option value="my">My Calendar</option>`;

  if (ME.role === "ADMIN") {
    html += `<optgroup label="Employee Calendars">`;
    html += USERS.map((u) => {
      return `<option value="user:${u.id}">${esc(u.full_name || u.username)} (${esc(u.role)})</option>`;
    }).join("");
    html += `</optgroup>`;
  }

  sel.innerHTML = html;

  if (!sel.dataset.initialized) {
    sel.value = "active";
    sel.dataset.initialized = "1";
  }

  updateViewModeNote();
}

function renderAudienceUsers() {
  const box = $("audienceUsersBox");
  if (!box) return;

  box.innerHTML = USERS.map((u) => `
    <label>
      <input type="checkbox" data-aud-user="${u.id}">
      <span>${esc(u.full_name || u.username)} <span class="tiny muted">(${esc(u.username)} • ${esc(u.role)})</span></span>
    </label>
  `).join("");

  toggleAudienceUsers();
}

async function loadMonth() {
  let url = `/api/calendar/month?month=${CURRENT_MONTH}`;

  if (isActiveView()) {
    url += `&view=ACTIVE`;
  } else if (isMyView()) {
    url += `&view=MY`;
  } else {
    url += `&view=USER&user_id=${selectedUserId()}`;
  }

  const out = await api(url);
  MONTH_DATA = out.data.days || [];
  renderMonth();
}

async function loadMonthSafe() {
  try {
    await loadMonth();
  } catch (err) {
    console.error("Calendar month load failed:", err);
    renderMonth();
  }
}

function filterRequestsForVisibleMonth(rows) {
  return (rows || []).filter((r) => {
    const start = String(r.start_date || "");
    const end = String(r.end_date || "");
    return start.startsWith(CURRENT_MONTH) || end.startsWith(CURRENT_MONTH);
  });
}

async function loadRequests() {
  const out = await api(`/api/calendar/requests?month=${CURRENT_MONTH}`);
  REQUESTS = filterRequestsForVisibleMonth(out.data || []);
  renderRequests();
  renderSelectedDay();
  updateHistoryMonthNote();
}

async function loadSchedules() {
  const wrap = $("scheduleEditor");
  if (!wrap) return;

  if (ME.role !== "ADMIN") {
    wrap.innerHTML = "";
    return;
  }

  if (isActiveView()) {
    renderScheduleEditor([]);
    return;
  }

  const out = await api(`/api/calendar/schedules?user_id=${selectedUserId()}`);
  renderScheduleEditor(out.data || []);
}

function renderMonth() {
  const monthGrid = $("monthGrid");
  const weekHead = $("weekHead");
  const monthTitle = $("monthTitle");

  if (!monthGrid || !weekHead || !monthTitle) return;

  monthTitle.textContent = monthLabel(CURRENT_MONTH);
  weekHead.innerHTML = weekNames().map((x) => `<div class="cal-week">${x}</div>`).join("");

  const map = new Map(MONTH_DATA.map((x) => [x.date, x]));
  const [y, m] = CURRENT_MONTH.split("-").map(Number);
  const first = new Date(y, m - 1, 1);
  const daysInMonth = new Date(y, m, 0).getDate();

  let offset = first.getDay();
  offset = offset === 0 ? 6 : offset - 1;

  let html = "";
  for (let i = 0; i < offset; i++) {
    html += `<div></div>`;
  }

  for (let d = 1; d <= daysInMonth; d++) {
    const dt = `${CURRENT_MONTH}-${String(d).padStart(2, "0")}`;
    const row = map.get(dt) || {
      schedule: [],
      unavailability: [],
      events: [],
      holidays: []
    };

    let working = 0;
    let unavailable = [];
    const events = row.events || [];
    const holidays = row.holidays || [];

    if (isActiveView()) {
      unavailable = (row.unavailability || []).filter(
        (u) => String(u.status || "").toUpperCase() === "APPROVED"
      );
    } else {
      working = (row.schedule || []).filter(
        (x) => Number(x.user_id) === selectedUserId() && Number(x.is_working) === 1
      ).length;

      unavailable = (row.unavailability || []).filter(
        (x) => Number(x.user_id) === selectedUserId()
      );
    }

    const holidayClass = holidays.length ? "holiday" : "";

    html += `
      <div class="cal-day ${dt === SELECTED_DATE ? "active" : ""} ${holidayClass}" data-date="${dt}">
        <div class="cal-day-head">
          <span class="cal-day-num">${d}</span>
          ${events.length ? `<span class="cal-mini">${events.length} event</span>` : ""}
        </div>
        <div>
          ${holidays.slice(0, 1).map((h) => `<span class="cal-pill holiday">${esc(h.title)}</span>`).join("")}
          ${!isActiveView() && working ? `<span class="cal-pill work">Work</span>` : ""}
          ${unavailable.slice(0, 2).map((u) => {
            const st = String(u.status || "").toUpperCase();
            const cls = st === "APPROVED"
              ? "approved"
              : st === "CANCEL_REQUESTED"
                ? "cancelreq"
                : "pending";

            const label = isActiveView()
              ? `${u.user_name || "User"} unavailable`
              : prettyStatus(u.status);

            return `<span class="cal-pill ${cls}">${esc(label)}</span>`;
          }).join("")}
          ${unavailable.length > 2 ? `<span class="cal-pill pending">+${unavailable.length - 2} more</span>` : ""}
          ${events.slice(0, 3).map((e) => `<span class="cal-pill event">${esc(e.title)}</span>`).join("")}
        </div>
      </div>
    `;
  }

  monthGrid.innerHTML = html;

  document.querySelectorAll("[data-date]").forEach((el) => {
    el.addEventListener("click", () => {
      SELECTED_DATE = el.dataset.date;
      renderMonth();
      renderSelectedDay();

      if ($("unStartDate")) $("unStartDate").value = SELECTED_DATE;
      if ($("unEndDate")) $("unEndDate").value = SELECTED_DATE;
      if ($("evDate")) $("evDate").value = SELECTED_DATE;
      if ($("holidayDate")) $("holidayDate").value = SELECTED_DATE;
    });
  });

  renderSelectedDay();
}

function renderSelectedDay() {
  const box = $("selectedDateList");
  const title = $("selectedDateText");
  if (!box || !title) return;

  title.textContent = `${SELECTED_DATE || "Choose a date"} • ${getSelectedCalendarLabel()}`;

  const row = MONTH_DATA.find((x) => x.date === SELECTED_DATE);
  if (!row) {
    box.innerHTML = `<div class="muted">No data</div>`;
    return;
  }

  let html = "";

  const holidays = row.holidays || [];
  if (holidays.length) {
    html += holidays.map((h) => `
      <div class="cal-item">
        <b>Holiday • ${esc(h.title)}</b>
        <div class="cal-mini">${esc(h.holiday_type || "")}</div>
        <div>${esc(h.notes || "")}</div>
        ${renderHolidayButtons(h)}
      </div>
    `).join("");
  }

  if (!isActiveView()) {
    const mySchedule = (row.schedule || []).filter((x) => Number(x.user_id) === selectedUserId());
    const myUn = (row.unavailability || []).filter((x) => Number(x.user_id) === selectedUserId());

    if (mySchedule.length) {
      html += mySchedule.map((s) => `
        <div class="cal-item">
          <b>Work Schedule</b>
          <div class="cal-mini">
            ${s.is_working ? formatTimeRange(s.start_time, s.end_time, 0) : "Off day"}
          </div>
        </div>
      `).join("");
    }

    if (myUn.length) {
      html += myUn.map((u) => `
        <div class="cal-item">
          <b>Unavailable</b>
          <div class="cal-mini">${esc(prettyStatus(u.status))} • ${formatTimeRange(u.start_time, u.end_time, u.all_day)}</div>
          <div>${esc(u.reason || "")}</div>
          ${renderReqButtons(u)}
        </div>
      `).join("");
    }
  } else {
    const sharedUn = (row.unavailability || []).filter(
      (u) => String(u.status || "").toUpperCase() === "APPROVED"
    );

    if (sharedUn.length) {
      html += sharedUn.map((u) => `
        <div class="cal-item">
          <b>${esc(u.user_name || "User")} • Unavailable</b>
          <div class="cal-mini">${esc(prettyStatus(u.status))} • ${formatTimeRange(u.start_time, u.end_time, u.all_day)}</div>
          <div>${esc(u.reason || "")}</div>
        </div>
      `).join("");
    }
  }

  const dayEvents = row.events || [];
  if (dayEvents.length) {
    html += dayEvents.map((e) => `
      <div class="cal-item">
        <b>${esc(e.title)}</b>
        <div class="cal-mini">${formatTimeRange(e.start_time, e.end_time, e.all_day)}</div>
        <div>${esc(e.notes || "")}</div>
        ${renderEventButtons(e)}
      </div>
    `).join("");
  }

  box.innerHTML = html || `<div class="muted">No items for this day.</div>`;

  bindReqButtons();
  bindEventButtons();
  bindHolidayButtons();
}

function renderReqButtons(u) {
  if (isActiveView()) return "";

  const status = String(u.status || "").toUpperCase();
  const meId = getCurrentUserId();

  if (ME.role !== "ADMIN" && Number(u.user_id) !== meId) return "";

  let btns = "";

  if (ME.role === "ADMIN" && (status === "PENDING" || status === "CANCEL_REQUESTED")) {
    btns += `
      <div class="row" style="margin-top:8px; flex-wrap:wrap;">
        <button class="btn mini" data-req-open-approve="${u.id}" type="button">Approve</button>
        <button class="btn ghost mini" data-req-action="DECLINE" data-req-id="${u.id}" type="button">Decline</button>
        <button class="btn ghost mini" data-req-edit="${u.id}" type="button">Edit</button>
        ${status === "CANCEL_REQUESTED" ? `<button class="btn mini danger" data-req-action="CONFIRM_CANCEL" data-req-id="${u.id}" type="button">Confirm Cancel</button>` : ""}
      </div>
      <div id="approveChoiceBox-${u.id}" class="cal-share-box cal-hidden" style="margin-top:8px;">
        <div style="font-size:13px; font-weight:600; color:#334155; margin-bottom:8px;">
          Approve request:
        </div>
        <div class="row" style="gap:8px; flex-wrap:wrap;">
          <button class="btn mini" data-req-approve-only="${u.id}" type="button">Approve Only</button>
          <button class="btn ghost mini" data-req-approve-share="${u.id}" type="button">Share with Active</button>
          <button class="btn ghost mini" data-req-approve-cancel="${u.id}" type="button">Cancel</button>
        </div>
        <div class="cal-mini" style="margin-top:8px;">
          Employee choice: ${Number(u.show_in_active || 0) === 1 ? "Wanted to share with Active" : "Did not request sharing"}
        </div>
      </div>
    `;
  }

  if (Number(u.user_id) === meId && ["PENDING", "APPROVED"].includes(status)) {
    btns += `
      <div class="row" style="margin-top:8px;">
        <button class="btn ghost mini" data-req-action="CANCEL" data-req-id="${u.id}" type="button">Cancel / Edit</button>
      </div>
    `;
  }

  return btns;
}

function renderEventButtons(e) {
  if (isActiveView()) return "";

  const canEdit = ME.role === "ADMIN" || String(e.created_by || "") === String(ME.user || "");
  if (!canEdit) return "";

  return `
    <div class="row" style="margin-top:8px;">
      <button class="btn ghost mini" data-edit-event="${e.id}" type="button">Edit</button>
      <button class="btn mini danger" data-delete-event="${e.id}" type="button">Delete</button>
    </div>
  `;
}

function renderHolidayButtons(h) {
  if (ME.role !== "ADMIN") return "";

  return `
    <div class="row" style="margin-top:8px;">
      <button class="btn ghost mini" data-edit-holiday="${h.id}" type="button">Edit</button>
    </div>
  `;
}

function fillUnavailabilityFormFromRequest(r) {
  if ($("unStartDate")) $("unStartDate").value = r.start_date || "";
  if ($("unEndDate")) $("unEndDate").value = r.end_date || "";
  if ($("unAllDay")) $("unAllDay").checked = Number(r.all_day || 0) === 1;
  if ($("unStartTime")) $("unStartTime").value = r.start_time || "";
  if ($("unEndTime")) $("unEndTime").value = r.end_time || "";
  if ($("unReason")) $("unReason").value = r.reason || "";
  if ($("unShareActive")) $("unShareActive").checked = Number(r.show_in_active || 0) === 1;
}

function hideAllApproveChoiceBoxes() {
  document.querySelectorAll('[id^="approveChoiceBox-"]').forEach((el) => {
    el.classList.add("cal-hidden");
  });
}

async function submitRequestAction(id, payload) {
  await api(`/api/calendar/unavailability/${id}/status`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });

  hideAllApproveChoiceBoxes();
  await loadMonth();
  await loadRequests();
  showMsg("unMsg", "Updated.", true);
}

function bindReqButtons() {
  document.querySelectorAll("[data-req-action]").forEach((btn) => {
    btn.onclick = async () => {
      const action = btn.dataset.reqAction;
      const id = btn.dataset.reqId;

      try {
        await submitRequestAction(id, { action });
      } catch (err) {
        showMsg("unMsg", err.message, false);
      }
    };
  });

  document.querySelectorAll("[data-req-open-approve]").forEach((btn) => {
    btn.onclick = () => {
      const id = btn.dataset.reqOpenApprove;
      const box = $(`approveChoiceBox-${id}`);
      hideAllApproveChoiceBoxes();
      if (box) box.classList.remove("cal-hidden");
    };
  });

  document.querySelectorAll("[data-req-approve-only]").forEach((btn) => {
    btn.onclick = async () => {
      const id = btn.dataset.reqApproveOnly;
      try {
        await submitRequestAction(id, {
          action: "APPROVE",
          show_in_active: 0
        });
      } catch (err) {
        showMsg("unMsg", err.message, false);
      }
    };
  });

  document.querySelectorAll("[data-req-approve-share]").forEach((btn) => {
    btn.onclick = async () => {
      const id = btn.dataset.reqApproveShare;
      try {
        await submitRequestAction(id, {
          action: "APPROVE",
          show_in_active: 1
        });
      } catch (err) {
        showMsg("unMsg", err.message, false);
      }
    };
  });

  document.querySelectorAll("[data-req-approve-cancel]").forEach((btn) => {
    btn.onclick = () => {
      hideAllApproveChoiceBoxes();
    };
  });

  document.querySelectorAll("[data-req-edit]").forEach((btn) => {
    btn.onclick = () => {
      const id = Number(btn.dataset.reqEdit);
      const req = REQUESTS.find((x) => Number(x.id) === id);
      if (!req) {
        showMsg("unMsg", "Request not found.", false);
        return;
      }

      fillUnavailabilityFormFromRequest(req);
      openActionPanel("unavailability");
      showMsg("unMsg", "Request loaded for review. Then use Approve / Decline in Selected Day.", true);
    };
  });
}

function bindEventButtons() {
  document.querySelectorAll("[data-edit-event]").forEach((btn) => {
    btn.onclick = () => {
      const id = Number(btn.dataset.editEvent);
      let found = null;

      for (const day of MONTH_DATA) {
        const ev = (day.events || []).find((x) => Number(x.id) === id);
        if (ev) {
          found = ev;
          break;
        }
      }

      if (!found) {
        showMsg("eventMsg", "Event not found.", false);
        return;
      }

      fillEventFormFromRow(found);
    };
  });

  document.querySelectorAll("[data-delete-event]").forEach((btn) => {
    btn.onclick = async () => {
      const id = Number(btn.dataset.deleteEvent);
      const yes = confirm("Delete this event?");
      if (!yes) return;

      try {
        await api(`/api/calendar/events/${id}`, {
          method: "DELETE"
        });

        if (EDIT_EVENT_ID === id) {
          clearEventForm();
        }

        await loadMonth();
        await loadPendingReminders();
        showMsg("eventMsg", "Event deleted.", true);
      } catch (err) {
        showMsg("eventMsg", err.message, false);
      }
    };
  });
}

function bindHolidayButtons() {
  document.querySelectorAll("[data-edit-holiday]").forEach((btn) => {
    btn.onclick = () => {
      const id = Number(btn.dataset.editHoliday);
      let found = null;

      for (const day of MONTH_DATA) {
        const item = (day.holidays || []).find((x) => Number(x.id) === id);
        if (item) {
          found = item;
          break;
        }
      }

      if (!found) {
        showMsg("holidayMsg", "Holiday not found.", false);
        return;
      }

      fillHolidayFormFromRow(found);
    };
  });
}

function renderScheduleEditor(rows) {
  const wrap = $("scheduleEditor");
  if (!wrap) return;

  if (ME.role !== "ADMIN") {
    wrap.innerHTML = "";
    return;
  }

  if (isActiveView()) {
    wrap.innerHTML = `<div class="muted">Schedule editing is not available in Active Calendar view.</div>`;
    return;
  }

  const map = new Map((rows || []).map((x) => [Number(x.weekday), x]));
  const names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];

  wrap.innerHTML = names.map((name, idx) => {
    const r = map.get(idx) || {
      weekday: idx,
      is_working: 0,
      start_time: "",
      end_time: "",
      allow_employee_edit: 1
    };

    return `
      <div class="cal-item">
        <div class="row" style="justify-content:space-between;">
          <b>${name}</b>
          <label class="cal-lock">
            <input type="checkbox" data-lock-day="${idx}" ${Number(r.allow_employee_edit || 0) === 1 ? "checked" : ""}>
            Employee can edit
          </label>
        </div>

        <div class="row">
          <label>
            <input type="checkbox" data-work-day="${idx}" ${Number(r.is_working || 0) === 1 ? "checked" : ""}>
            Working day
          </label>
        </div>

        <div class="grid2">
          <div><input type="time" data-start-day="${idx}" value="${esc(r.start_time || "")}"></div>
          <div><input type="time" data-end-day="${idx}" value="${esc(r.end_time || "")}"></div>
        </div>
      </div>
    `;
  }).join("");
}

async function saveSchedule() {
  if (ME.role !== "ADMIN") {
    showMsg("scheduleMsg", "Only admin can manage work schedules.", false);
    return;
  }

  if (isActiveView()) {
    showMsg("scheduleMsg", "You cannot edit schedule in Active Calendar view.", false);
    return;
  }

  try {
    const rows = [];

    for (let i = 0; i < 7; i++) {
      rows.push({
        weekday: i,
        is_working: !!document.querySelector(`[data-work-day="${i}"]`)?.checked,
        start_time: document.querySelector(`[data-start-day="${i}"]`)?.value || "",
        end_time: document.querySelector(`[data-end-day="${i}"]`)?.value || "",
        allow_employee_edit: document.querySelector(`[data-lock-day="${i}"]`)?.checked ? 1 : 0
      });
    }

    await api("/api/calendar/schedules", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        user_id: selectedUserId(),
        rows
      })
    });

    showMsg("scheduleMsg", "Schedule saved.", true);
    await loadMonth();
  } catch (err) {
    showMsg("scheduleMsg", err.message, false);
  }
}

async function saveUnavailability() {
  if (isActiveView()) {
    showMsg("unMsg", "Please switch to My Calendar to create a break request.", false);
    return;
  }

  try {
    await api("/api/calendar/unavailability", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        user_id: selectedUserId(),
        start_date: $("unStartDate")?.value || "",
        end_date: $("unEndDate")?.value || "",
        all_day: $("unAllDay")?.checked || false,
        start_time: $("unStartTime")?.value || "",
        end_time: $("unEndTime")?.value || "",
        reason: $("unReason")?.value || "",
        show_in_active: $("unShareActive")?.checked ? 1 : 0
      })
    });

    showMsg("unMsg", "Temporary request saved. Inform HR/Admin.", true);
    await loadMonth();
    await loadRequests();
  } catch (err) {
    showMsg("unMsg", err.message, false);
  }
}

function buildReminderTimes() {
  const times = [];

  const mainEventDate = $("evDate")?.value || "";
  const mainReminderTime = $("mainReminderTime")?.value || "";
  if (mainEventDate && mainReminderTime) {
    times.push(`${mainEventDate} ${mainReminderTime}`);
  }

  const pairs = [
    { d: $("rem1Date")?.value || "", t: $("rem1Time")?.value || "" },
    { d: $("rem2Date")?.value || "", t: $("rem2Time")?.value || "" },
    { d: $("rem3Date")?.value || "", t: $("rem3Time")?.value || "" }
  ];

  pairs.forEach((x) => {
    if (x.d && x.t) {
      times.push(`${x.d} ${x.t}`);
    }
  });

  return times;
}

async function saveEvent() {
  if (isActiveView()) {
    showMsg("eventMsg", "Please switch to My Calendar to create or edit events.", false);
    return;
  }

  try {
    const audience_user_ids = [...document.querySelectorAll("[data-aud-user]:checked")].map((x) =>
      Number(x.dataset.audUser)
    );

    const reminder_times = buildReminderTimes();

    const payload = {
      calendar_scope: scope(),
      title: $("evTitle")?.value || "",
      event_date: $("evDate")?.value || "",
      all_day: $("evAllDay")?.checked || false,
      start_time: $("evStart")?.value || "",
      end_time: $("evEnd")?.value || "",
      notes: $("evNotes")?.value || "",
      audience_type: $("audienceType")?.value || "ONLY_ME",
      audience_user_ids,
      reminder_times,
      show_in_active: $("evShowInActive")?.checked ? 1 : 0
    };

    if (EDIT_EVENT_ID) {
      await api(`/api/calendar/events/${EDIT_EVENT_ID}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });

      showMsg("eventMsg", "Event updated.", true);
    } else {
      await api("/api/calendar/events", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });

      showMsg("eventMsg", "Event / reminder saved.", true);
    }

    await loadMonth();
    await loadPendingReminders();
    clearEventForm();
  } catch (err) {
    showMsg("eventMsg", err.message, false);
  }
}

async function saveHoliday() {
  try {
    await api("/api/calendar/holidays", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        title: $("holidayTitle")?.value || "",
        holiday_date: $("holidayDate")?.value || "",
        holiday_type: $("holidayType")?.value || "GOVERNMENT_HOLIDAY",
        notes: $("holidayNotes")?.value || ""
      })
    });

    showMsg("holidayMsg", "Holiday saved.", true);
    await loadMonth();
    clearHolidayForm();
  } catch (err) {
    showMsg("holidayMsg", err.message, false);
  }
}

async function updateHoliday() {
  if (!EDIT_HOLIDAY_ID) {
    showMsg("holidayMsg", "Select a holiday first.", false);
    return;
  }

  try {
    await api(`/api/calendar/holidays/${EDIT_HOLIDAY_ID}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        title: $("holidayTitle")?.value || "",
        holiday_date: $("holidayDate")?.value || "",
        holiday_type: $("holidayType")?.value || "GOVERNMENT_HOLIDAY",
        notes: $("holidayNotes")?.value || ""
      })
    });

    showMsg("holidayMsg", "Holiday updated.", true);
    await loadMonth();
    clearHolidayForm();
  } catch (err) {
    showMsg("holidayMsg", err.message, false);
  }
}

async function deleteHoliday() {
  if (!EDIT_HOLIDAY_ID) {
    showMsg("holidayMsg", "Select a holiday first.", false);
    return;
  }

  const yes = confirm("Delete this holiday?");
  if (!yes) return;

  try {
    await api(`/api/calendar/holidays/${EDIT_HOLIDAY_ID}`, {
      method: "DELETE"
    });

    showMsg("holidayMsg", "Holiday deleted.", true);
    await loadMonth();
    clearHolidayForm();
  } catch (err) {
    showMsg("holidayMsg", err.message, false);
  }
}

function renderRequests() {
  const box = $("requestList");
  if (!box) return;

  if (!REQUESTS.length) {
    box.innerHTML = `<div class="muted">No requests for this month.</div>`;
    return;
  }

  box.innerHTML = REQUESTS.map((r) => `
    <div class="cal-item">
      <label class="row" style="gap:8px; align-items:flex-start;">
        <input type="checkbox" data-history-id="${r.id}">
        <span style="flex:1;">
          <b>${esc(r.user_name)}</b>
          <div class="cal-mini">${esc(r.start_date)} → ${esc(r.end_date)} • ${esc(prettyStatus(r.status))}</div>
          <div>${esc(r.reason || "")}</div>
          <div class="cal-mini">Employee share choice: ${Number(r.show_in_active || 0) === 1 ? "Share to Active" : "Do not share"}</div>
          ${ME.role === "ADMIN" ? `
            <div class="row" style="margin-top:8px;">
              <button class="btn ghost mini" data-history-edit="${r.id}" type="button">Edit</button>
            </div>
          ` : ""}
        </span>
      </label>
    </div>
  `).join("");

  document.querySelectorAll("[data-history-edit]").forEach((btn) => {
    btn.onclick = () => {
      const id = Number(btn.dataset.historyEdit);
      const req = REQUESTS.find((x) => Number(x.id) === id);
      if (!req) {
        showMsg("historyMsg", "Request not found.", false);
        return;
      }

      fillUnavailabilityFormFromRequest(req);
      openActionPanel("unavailability");
      showMsg("unMsg", "Request loaded for review. Then use Approve / Decline in Selected Day.", true);
    };
  });
}

async function deleteSelectedHistory() {
  const ids = [...document.querySelectorAll("[data-history-id]:checked")].map((el) =>
    Number(el.dataset.historyId)
  );

  if (!ids.length) {
    showMsg("historyMsg", "Select at least one history item.", false);
    return;
  }

  const yes = confirm("Delete selected history items?");
  if (!yes) return;

  try {
    await api("/api/calendar/requests/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ids })
    });

    await loadRequests();
    await loadMonth();
    showMsg("historyMsg", "Selected history deleted.", true);
  } catch (err) {
    showMsg("historyMsg", err.message, false);
  }
}

async function loadPendingReminders() {
  if (document.hidden) return;

  try {
    const out = await api("/api/calendar/reminders/pending");
    const box = $("pendingReminders");
    if (!box) return;

    const rows = out.data || [];
    if (!rows.length) {
      box.innerHTML = "";
      return;
    }

    const canNotify = await ensureNotificationPermission();

    rows.forEach((r) => {
      const key = `${r.event_id}|${r.remind_at}`;
      if (canNotify && !SHOWN_REMINDERS.has(key)) {
        new Notification(`Reminder: ${r.title}`, {
          body: `${r.event_date} • ${String(r.remind_at || "").replace("T", " ")}`
        });
        SHOWN_REMINDERS.add(key);
      }
    });

    box.innerHTML = rows.slice(0, 4).map((r) => `
      <div class="pending-card">
        <div><b>${esc(r.title)}</b></div>
        <div class="cal-mini">${esc(r.event_date)} • ${esc(String(r.remind_at || "").replace("T", " "))}</div>
        <div class="row" style="margin-top:10px;">
          <button class="btn mini" data-ack-event="${r.event_id}" data-ack-time="${r.remind_at}" type="button">OK</button>
        </div>
      </div>
    `).join("");

    document.querySelectorAll("[data-ack-event]").forEach((btn) => {
      btn.onclick = async () => {
        await api("/api/calendar/reminders/ack", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            event_id: Number(btn.dataset.ackEvent),
            remind_at: btn.dataset.ackTime
          })
        });

        SHOWN_REMINDERS.delete(`${btn.dataset.ackEvent}|${btn.dataset.ackTime}`);
        await loadPendingReminders();
      };
    });
  } catch (e) {}
}

function shiftMonth(delta) {
  const [y, m] = CURRENT_MONTH.split("-").map(Number);
  const d = new Date(y, m - 1 + delta, 1);
  CURRENT_MONTH = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
  updateHistoryMonthNote();
  loadMonthSafe();
  loadRequests();
}



document.addEventListener("DOMContentLoaded", async () => {
  await loadMe();
  await loadUsers();
  updateViewModeNote();
  updateHistoryMonthNote();
  updatePanelsByView();

  renderMonth();

  await loadMonthSafe();
  await loadSchedules();
  await loadRequests();
  await ensureNotificationPermission();
  await loadPendingReminders();

  if (ME.role === "ADMIN") {
    openActionPanel("schedule");
  } else {
    openActionPanel("unavailability");
  }

  toggleAudienceUsers();
  clearEventForm();
  clearHolidayForm();

  $("calendarActionSelect")?.addEventListener("change", () => {
    openActionPanel($("calendarActionSelect").value);
  });

  $("audienceType")?.addEventListener("change", toggleAudienceUsers);

  $("viewUserSelect")?.addEventListener("change", async () => {
    updateViewModeNote();
    updatePanelsByView();
    await loadSchedules();
    await loadMonthSafe();
    await loadRequests();
  });

  $("prevMonthBtn")?.addEventListener("click", () => shiftMonth(-1));
  $("nextMonthBtn")?.addEventListener("click", () => shiftMonth(1));

  $("todayBtn")?.addEventListener("click", async () => {
    const now = localDateParts();
    CURRENT_MONTH = now.month;
    SELECTED_DATE = now.date;
    updateHistoryMonthNote();
    await loadMonthSafe();
    await loadRequests();
  });

  $("saveScheduleBtn")?.addEventListener("click", saveSchedule);
  $("saveUnBtn")?.addEventListener("click", saveUnavailability);
  $("saveEventBtn")?.addEventListener("click", saveEvent);
  $("clearEventBtn")?.addEventListener("click", clearEventForm);
  $("saveHolidayBtn")?.addEventListener("click", saveHoliday);
  $("updateHolidayBtn")?.addEventListener("click", updateHoliday);
  $("deleteHolidayBtn")?.addEventListener("click", deleteHoliday);
  $("clearHolidayBtn")?.addEventListener("click", clearHolidayForm);

  $("refreshHistoryBtn")?.addEventListener("click", async () => {
    await loadRequests();
    showMsg("historyMsg", "History refreshed.", true);
  });

  $("deleteHistoryBtn")?.addEventListener("click", deleteSelectedHistory);

  if ($("unStartDate")) $("unStartDate").value = SELECTED_DATE;
  if ($("unEndDate")) $("unEndDate").value = SELECTED_DATE;
  if ($("evDate")) $("evDate").value = SELECTED_DATE;
  if ($("holidayDate")) $("holidayDate").value = SELECTED_DATE;

  setInterval(loadPendingReminders, 30000);

  document.addEventListener("visibilitychange", async () => {
    if (!document.hidden) {
      await loadPendingReminders();
    }
  });
});
