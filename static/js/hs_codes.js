const $ = (id) => document.getElementById(id);

let HS_ROWS = [];
let EDIT_ID = null;
let searchTimer = null;

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

function esc(s) {
  return String(s || "")
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

function clearForm() {
  EDIT_ID = null;

  if ($("productName")) $("productName").value = "";
  if ($("hsCode")) $("hsCode").value = "";
  if ($("proofLink")) $("proofLink").value = "";
  if ($("notes")) $("notes").value = "";

  if ($("formTitle")) $("formTitle").textContent = "Add HS Code";
  if ($("saveBtn")) $("saveBtn").disabled = false;
  if ($("updateBtn")) $("updateBtn").disabled = true;
  if ($("deleteBtn")) $("deleteBtn").disabled = true;

  showMsg("formMsg", "", true);
}

function fillForm(row) {
  EDIT_ID = Number(row.id);

  if ($("productName")) $("productName").value = row.product_name || "";
  if ($("hsCode")) $("hsCode").value = row.hs_code || "";
  if ($("proofLink")) $("proofLink").value = row.proof_link || "";
  if ($("notes")) $("notes").value = row.notes || "";

  if ($("formTitle")) $("formTitle").textContent = "Edit HS Code";
  if ($("saveBtn")) $("saveBtn").disabled = true;
  if ($("updateBtn")) $("updateBtn").disabled = false;
  if ($("deleteBtn")) $("deleteBtn").disabled = false;
}

function updateBottomScrollWidth() {
  const table = document.querySelector(".hs-table");
  const inner = $("hsScrollInner");
  if (!table || !inner) return;
  inner.style.width = table.scrollWidth + "px";
}

function setupScrollSync() {
  const tableWrap = $("hsTableWrap");
  const bottomScroll = $("hsBottomScroll");

  if (!tableWrap || !bottomScroll) return;

  let syncingTop = false;
  let syncingBottom = false;

  tableWrap.addEventListener("scroll", () => {
    if (syncingBottom) {
      syncingBottom = false;
      return;
    }
    syncingTop = true;
    bottomScroll.scrollLeft = tableWrap.scrollLeft;
  });

  bottomScroll.addEventListener("scroll", () => {
    if (syncingTop) {
      syncingTop = false;
      return;
    }
    syncingBottom = true;
    tableWrap.scrollLeft = bottomScroll.scrollLeft;
  });

  updateBottomScrollWidth();
  window.addEventListener("resize", updateBottomScrollWidth);
}

function renderTable() {
  const body = $("tableBody");
  if (!body) return;

  if (!HS_ROWS.length) {
    body.innerHTML = `<tr><td colspan="7" class="hs-empty">No HS code records found.</td></tr>`;
    updateBottomScrollWidth();
    return;
  }

  body.innerHTML = HS_ROWS.map((r) => {
    const fixedLink = normalizeLink(r.proof_link || "");
    return `
      <tr>
        <td>
          <div><b>${esc(r.product_name)}</b></div>
          <div class="hs-mini">Created: ${esc(r.created_at || "")}</div>
        </td>
        <td>
          <span class="hs-code">${esc(r.hs_code)}</span>
        </td>
        <td>
          ${
            fixedLink
              ? `<a class="hs-link" href="${esc(fixedLink)}" target="_blank" rel="noopener noreferrer">${esc(r.proof_link || "")}</a>`
              : `<span class="hs-mini">No proof link</span>`
          }
        </td>
        <td>${esc(r.notes || "")}</td>
        <td>
          <div>${esc(r.created_by || "")}</div>
          <div class="hs-mini">${esc(r.created_at || "")}</div>
        </td>
        <td>
          <div>${esc(r.updated_by || "")}</div>
          <div class="hs-mini">${esc(r.updated_at || "")}</div>
        </td>
        <td>
          <div class="hs-row-actions">
            <button class="btn ghost mini" type="button" data-edit="${r.id}">Edit</button>
            <button class="btn ghost mini" type="button" data-delete="${r.id}">Delete</button>
          </div>
        </td>
      </tr>
    `;
  }).join("");

  document.querySelectorAll("[data-edit]").forEach((btn) => {
    btn.onclick = () => {
      const id = Number(btn.dataset.edit);
      const row = HS_ROWS.find((x) => Number(x.id) === id);
      if (!row) return;
      fillForm(row);
      showMsg("formMsg", "Record loaded for editing.", true);
    };
  });

  document.querySelectorAll("[data-delete]").forEach((btn) => {
    btn.onclick = async () => {
      const id = Number(btn.dataset.delete);
      const yes = confirm("Delete this HS code record?");
      if (!yes) return;

      try {
        await api(`/api/hs-codes/${id}`, { method: "DELETE" });
        await loadRows();
        if (EDIT_ID === id) clearForm();
        showMsg("tableMsg", "Record deleted.", true);
      } catch (err) {
        showMsg("tableMsg", err.message, false);
      }
    };
  });

  updateBottomScrollWidth();
}

function getPayload() {
  return {
    product_name: $("productName")?.value || "",
    hs_code: $("hsCode")?.value || "",
    proof_link: $("proofLink")?.value || "",
    notes: $("notes")?.value || ""
  };
}

async function loadRows() {
  const q = $("searchInput")?.value.trim() || "";
  const out = await api(`/api/hs-codes?q=${encodeURIComponent(q)}`);
  HS_ROWS = out.data || [];
  renderTable();
}

async function saveRow() {
  try {
    await api("/api/hs-codes", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(getPayload())
    });

    await loadRows();
    clearForm();
    showMsg("formMsg", "HS code added.", true);
  } catch (err) {
    showMsg("formMsg", err.message, false);
  }
}

async function updateRow() {
  if (!EDIT_ID) {
    showMsg("formMsg", "Select a record first.", false);
    return;
  }

  try {
    await api(`/api/hs-codes/${EDIT_ID}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(getPayload())
    });

    await loadRows();
    clearForm();
    showMsg("formMsg", "HS code updated.", true);
  } catch (err) {
    showMsg("formMsg", err.message, false);
  }
}

async function deleteRow() {
  if (!EDIT_ID) {
    showMsg("formMsg", "Select a record first.", false);
    return;
  }

  const yes = confirm("Delete this HS code record?");
  if (!yes) return;

  try {
    await api(`/api/hs-codes/${EDIT_ID}`, {
      method: "DELETE"
    });

    await loadRows();
    clearForm();
    showMsg("formMsg", "HS code deleted.", true);
  } catch (err) {
    showMsg("formMsg", err.message, false);
  }
}

function scheduleSearchReload() {
  if (searchTimer) {
    clearTimeout(searchTimer);
  }

  searchTimer = setTimeout(() => {
    loadRows();
  }, 250);
}

document.addEventListener("DOMContentLoaded", async () => {
  $("saveBtn")?.addEventListener("click", saveRow);
  $("updateBtn")?.addEventListener("click", updateRow);
  $("deleteBtn")?.addEventListener("click", deleteRow);
  $("clearBtn")?.addEventListener("click", clearForm);

  $("searchBtn")?.addEventListener("click", loadRows);

  $("refreshBtn")?.addEventListener("click", async () => {
    if ($("searchInput")) $("searchInput").value = "";
    await loadRows();
    showMsg("tableMsg", "List refreshed.", true);
  });

  $("searchInput")?.addEventListener("keydown", async (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      await loadRows();
    }
  });

  $("searchInput")?.addEventListener("input", scheduleSearchReload);

  setupScrollSync();
  clearForm();
  await loadRows();
});
