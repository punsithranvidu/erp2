const $ = (id) => document.getElementById(id);

let CURRENT_TYPE = "INV";
let CURRENT_LIVE_NUMBER = "";
let PENDING_RESERVED_NUMBER = null;
let PENDING_RESTORE_NUMBER = null;

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

function showMsg(id, text, ok = true) {
  const el = $(id);
  if (!el) return;
  el.textContent = text || "";
  el.className = "msg " + (ok ? "ok" : "bad");
}

function setActiveType(type) {
  CURRENT_TYPE = type;

  $("typeINV").className = type === "INV" ? "btn active" : "btn ghost";
  $("typeQT").className = type === "QT" ? "btn active" : "btn ghost";
  $("typePO").className = type === "PO" ? "btn active" : "btn ghost";

  loadLiveNumber();
}

async function loadLiveNumber() {
  try {
    const out = await api(`/api/invoices/next?type=${encodeURIComponent(CURRENT_TYPE)}`);
    CURRENT_LIVE_NUMBER = out.number || "";
    $("liveNumber").textContent = CURRENT_LIVE_NUMBER || "N/A";
  } catch (err) {
    $("liveNumber").textContent = "Error";
    showMsg("topMsg", err.message, false);
  }
}

async function copyTextCrossPlatform(text) {
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {}

  const ta = document.createElement("textarea");
  ta.value = text;
  ta.setAttribute("readonly", "");
  ta.style.position = "fixed";
  ta.style.left = "-9999px";
  ta.style.top = "0";
  document.body.appendChild(ta);

  ta.focus();
  ta.select();
  ta.setSelectionRange(0, ta.value.length);

  let ok = false;
  try {
    ok = document.execCommand("copy");
  } catch {
    ok = false;
  }

  document.body.removeChild(ta);
  return ok;
}

function openModal(id) {
  $(id)?.classList.add("open");
}

function closeModal(id) {
  $(id)?.classList.remove("open");
}

async function reserveAndCopyCurrentNumber() {
  try {
    showMsg("topMsg", "", true);
    showMsg("copyModalMsg", "", true);

    const out = await api("/api/invoices/reserve", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ type: CURRENT_TYPE })
    });

    const number = out.number;
    PENDING_RESERVED_NUMBER = number;

    const copied = await copyTextCrossPlatform(number);
    $("copiedNumberText").textContent = number;

    openModal("copyModal");

    if (copied) {
      showMsg("copyModalMsg", "Copied successfully.", true);
    } else {
      showMsg("copyModalMsg", "Could not auto-copy. You can still copy this number manually.", false);
    }
  } catch (err) {
    showMsg("topMsg", err.message, false);
  }
}

async function confirmCopiedNumber() {
  try {
    if (!PENDING_RESERVED_NUMBER) {
      closeModal("copyModal");
      return;
    }

    await api("/api/invoices/use", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ number: PENDING_RESERVED_NUMBER })
    });

    closeModal("copyModal");
    showMsg("topMsg", `Finalized: ${PENDING_RESERVED_NUMBER}`, true);

    PENDING_RESERVED_NUMBER = null;
    await loadLiveNumber();
  } catch (err) {
    showMsg("copyModalMsg", err.message, false);
  }
}

async function undoCopiedNumber() {
  try {
    if (!PENDING_RESERVED_NUMBER) {
      closeModal("copyModal");
      return;
    }

    await api("/api/invoices/restore", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ number: PENDING_RESERVED_NUMBER })
    });

    closeModal("copyModal");
    showMsg("topMsg", `Restored: ${PENDING_RESERVED_NUMBER}`, true);

    PENDING_RESERVED_NUMBER = null;
    await loadLiveNumber();
  } catch (err) {
    showMsg("copyModalMsg", err.message, false);
  }
}

function askRestoreNumber() {
  const value = ($("restoreInput").value || "").trim().toUpperCase();
  if (!value) {
    showMsg("restoreMsg", "Enter a number to restore.", false);
    return;
  }

  PENDING_RESTORE_NUMBER = value;
  $("restoreModalNumber").textContent = value;
  showMsg("restoreModalMsg", "", true);
  openModal("restoreModal");
}

async function confirmRestoreNumber() {
  try {
    if (!PENDING_RESTORE_NUMBER) {
      closeModal("restoreModal");
      return;
    }

    await api("/api/invoices/restore", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ number: PENDING_RESTORE_NUMBER })
    });

    showMsg("restoreMsg", `Restored: ${PENDING_RESTORE_NUMBER}`, true);
    closeModal("restoreModal");

    $("restoreInput").value = "";
    PENDING_RESTORE_NUMBER = null;
    await loadLiveNumber();
  } catch (err) {
    showMsg("restoreModalMsg", err.message, false);
  }
}

function cancelRestoreNumber() {
  PENDING_RESTORE_NUMBER = null;
  closeModal("restoreModal");
}

async function searchNumber() {
  try {
    const q = ($("searchInput").value || "").trim().toUpperCase();
    if (!q) {
      $("searchResult").textContent = "Enter a number to search.";
      return;
    }

    const out = await api(`/api/invoices/search?q=${encodeURIComponent(q)}`);
    if (!out.found) {
      $("searchResult").innerHTML = `<b>${q}</b><br>Not found in the system.`;
      return;
    }

    const status = String(out.status || "").toUpperCase();

    $("searchResult").innerHTML = `
      <b>${q}</b><br>
      Found in system.<br>
      Current status: <b>${status}</b>
    `;
  } catch (err) {
    $("searchResult").textContent = err.message;
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  $("typeINV")?.addEventListener("click", () => setActiveType("INV"));
  $("typeQT")?.addEventListener("click", () => setActiveType("QT"));
  $("typePO")?.addEventListener("click", () => setActiveType("PO"));

  $("refreshBtn")?.addEventListener("click", loadLiveNumber);
  $("copyBtn")?.addEventListener("click", reserveAndCopyCurrentNumber);

  $("copyOkBtn")?.addEventListener("click", confirmCopiedNumber);
  $("copyUndoBtn")?.addEventListener("click", undoCopiedNumber);

  $("restoreBtn")?.addEventListener("click", askRestoreNumber);
  $("restoreOkBtn")?.addEventListener("click", confirmRestoreNumber);
  $("restoreCancelBtn")?.addEventListener("click", cancelRestoreNumber);

  $("searchBtn")?.addEventListener("click", searchNumber);

  $("searchInput")?.addEventListener("keydown", async (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      await searchNumber();
    }
  });

  $("restoreInput")?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      askRestoreNumber();
    }
  });

  await loadLiveNumber();
});