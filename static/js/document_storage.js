const $ = (id) => document.getElementById(id);

let ME = null;
let USERS = [];
let CURRENT_PARENT_ID = null;
let CURRENT_ITEMS = [];
let EDITING_ID = null;
let EDITING_ITEM = null;
let PATH_STACK = [];
let VIEW_MODE = "MY"; // MY | SHARED | TRASH
let TRASH_SELECTED = new Set();
let SHARED_TREE_MODE = false;

let creatingFolder = false;
let uploadingDocument = false;

function safeText(v){ return (v === null || v === undefined) ? "" : String(v); }

async function safeJson(res){
  const ct = (res.headers.get("content-type") || "").toLowerCase();
  if (ct.includes("application/json")) return await res.json();
  const text = await res.text();
  throw new Error("Server returned non-JSON. " + text.slice(0, 220));
}

function showMsg(id, text, ok = true){
  const el = $(id);
  if(!el) return;
  el.textContent = text || "";
  el.className = "msg " + (ok ? "ok" : "bad");
}

function getMeRole(){
  if(!ME) return "";
  return ME.role || (ME.data && ME.data.role) || "";
}

function getMeUser(){
  if(!ME) return "";
  return ME.user || (ME.data && ME.data.user) || "";
}

function setViewButtons(){
  $("myFoldersBtn").className = (VIEW_MODE === "MY" && !SHARED_TREE_MODE) ? "btn mini" : "btn ghost mini";
  $("sharedBtn").className = (VIEW_MODE === "SHARED" || SHARED_TREE_MODE) ? "btn mini" : "btn ghost mini";

  const trashBtn = $("trashBtn");
  if(trashBtn){
    trashBtn.className = VIEW_MODE === "TRASH" ? "btn mini" : "btn ghost mini";
  }
}

function setTrashToolbar(){
  const trashMode = VIEW_MODE === "TRASH";

  const ids = [
    "trashSelectAllBtn",
    "trashClearAllBtn",
    "trashRestoreSelectedBtn",
    "trashDeleteSelectedBtn",
    "trashCheckHead"
  ];

  ids.forEach(id => {
    const el = $(id);
    if(el) el.style.display = trashMode ? "" : "none";
  });
}

async function getMe(){
  const res = await fetch("/api/me", { credentials: "same-origin" });
  ME = await safeJson(res);
}

async function loadUsers(){
  const res = await fetch("/api/docs/users", { credentials: "same-origin" });
  const out = await safeJson(res);
  USERS = out.data || [];
  renderCreatePermUsers();
  renderEditPermUsers([]);
}

function filteredUsers(searchText){
  const q = (searchText || "").trim().toLowerCase();
  return USERS.filter(u => {
    const txt = `${u.username} ${u.full_name || ""} ${u.role} ${u.google_email || ""}`.toLowerCase();
    return !q || txt.includes(q);
  });
}

function makeUserPermissionRow(u, accessChecked = false, editChecked = false, prefix = "create"){
  const isAdmin = u.role === "ADMIN";
  const isMe = getMeUser() && u.username === getMeUser();
  const forced = isAdmin || isMe;
  const disabled = forced ? "disabled" : "";
  const sub = [u.full_name || "", u.google_email || "No Google email"].filter(Boolean).join(" • ");

  return `
    <div class="doc-share-row">
      <div class="doc-share-user">
        <div>
          <b>${safeText(u.username)}</b>
          <div class="tiny muted">${safeText(sub)}</div>
        </div>
        <span class="role-badge ${u.role === "ADMIN" ? "role-admin" : "role-emp"}">${u.role}</span>
      </div>

      <div class="row">
        <label class="row tiny" style="gap:6px;">
          <input type="checkbox" data-${prefix}-access="${u.id}" ${forced || accessChecked ? "checked" : ""} ${disabled}/>
          view
        </label>
        <label class="row tiny" style="gap:6px;">
          <input type="checkbox" data-${prefix}-edit="${u.id}" ${forced || editChecked ? "checked" : ""} ${disabled}/>
          edit
        </label>
      </div>
    </div>
  `;
}

function renderCreatePermUsers(){
  const box = $("shareUserBox");
  const search = $("shareSearch").value || "";
  box.innerHTML = filteredUsers(search)
    .map(u => makeUserPermissionRow(u, false, false, "create"))
    .join("");
}

function renderEditPermUsers(perms){
  const box = $("editPermUserBox");
  const permMap = {};
  (perms || []).forEach(p => { permMap[String(p.user_id)] = p; });

  box.innerHTML = USERS.map(u => {
    const p = permMap[String(u.id)] || {};
    return makeUserPermissionRow(u, Number(p.can_access) === 1, Number(p.can_edit) === 1, "edit");
  }).join("");
}

function collectPermissions(prefix){
  const out = [];
  USERS.forEach(u => {
    const accessEl = document.querySelector(`[data-${prefix}-access="${u.id}"]`);
    const editEl = document.querySelector(`[data-${prefix}-edit="${u.id}"]`);
    if(!accessEl || !editEl) return;

    const can_access = accessEl.checked ? 1 : 0;
    const can_edit = (can_access && editEl.checked) ? 1 : 0;

    if(can_access || can_edit || u.role === "ADMIN" || (getMeUser() && u.username === getMeUser())){
      out.push({ user_id: u.id, can_access, can_edit });
    }
  });
  return out;
}

function renderPath(){
  if(VIEW_MODE === "TRASH"){
    $("pathText").textContent = "Trash Bin";
    return;
  }

  if(VIEW_MODE === "SHARED" && !SHARED_TREE_MODE){
    $("pathText").textContent = "Shared With Me";
    return;
  }

  if(SHARED_TREE_MODE){
    if(PATH_STACK.length === 0){
      $("pathText").textContent = "Shared With Me";
      return;
    }
    $("pathText").textContent = "Shared With Me / " + PATH_STACK.map(p => p.name).join(" / ");
    return;
  }

  if(PATH_STACK.length === 0){
    $("pathText").textContent = "Root";
    return;
  }

  $("pathText").textContent = PATH_STACK.map(p => p.name).join(" / ");
}

function setTrashButtons(show){
  const restoreBtn = $("restoreItemBtn");
  const deleteForeverBtn = $("deleteForeverBtn");
  const deleteBtn = $("deleteItemBtn");

  if(restoreBtn) restoreBtn.style.display = show ? "" : "none";
  if(deleteForeverBtn) deleteForeverBtn.style.display = show ? "" : "none";
  if(deleteBtn) deleteBtn.style.display = show ? "none" : "";
}

function getWorkingParentId(){
  if(VIEW_MODE === "TRASH") return null;

  if(CURRENT_PARENT_ID !== null && CURRENT_PARENT_ID !== undefined){
    return CURRENT_PARENT_ID;
  }

  if(EDITING_ITEM && EDITING_ITEM.item_type === "FOLDER" && !EDITING_ITEM.deleted_at){
    return EDITING_ITEM.id;
  }

  return null;
}

async function loadItems(){
  try{
    renderPath();
    setViewButtons();
    setTrashToolbar();

    let url;

    if(VIEW_MODE === "SHARED" && !SHARED_TREE_MODE){
      const q = encodeURIComponent($("listSearch").value || "");
      url = `/api/docs/shared?q=${q}`;
    }else if(VIEW_MODE === "TRASH"){
      const q = encodeURIComponent($("listSearch").value || "");
      url = `/api/docs/trash?q=${q}`;
    }else{
      const params = new URLSearchParams();
      params.set("parent_id", CURRENT_PARENT_ID === null ? "ROOT" : String(CURRENT_PARENT_ID));
      params.set("q", $("listSearch").value || "");
      url = `/api/docs/items?${params.toString()}`;
    }

    const res = await fetch(url, { credentials: "same-origin" });
    const out = await safeJson(res);
    if(!res.ok) throw new Error(out.error || "Failed to load items");

    CURRENT_ITEMS = out.data || [];
    renderItems(CURRENT_ITEMS);
    showMsg("listMsg", `${CURRENT_ITEMS.length} item(s) loaded.`, true);
  }catch(err){
    showMsg("listMsg", err.message, false);
  }
}

function openFolderInExplorer(item){
  if(!item || item.item_type !== "FOLDER") return;

  if(VIEW_MODE === "SHARED" && !SHARED_TREE_MODE){
    SHARED_TREE_MODE = true;
    VIEW_MODE = "MY";
    PATH_STACK = [{ id: item.id, name: item.name }];
    CURRENT_PARENT_ID = item.id;
    clearSelection(false);
    setViewButtons();
    loadItems();
    return;
  }

  if(!PATH_STACK.some(x => String(x.id) === String(item.id))){
    PATH_STACK.push({ id: item.id, name: item.name });
  }
  CURRENT_PARENT_ID = item.id;
  clearSelection(false);
  loadItems();
}

function renderItems(rows){
  const body = $("docsBody");
  body.innerHTML = "";

  const colCount = VIEW_MODE === "TRASH" ? 9 : 8;

  if(!rows.length){
    body.innerHTML = `<tr><td colspan="${colCount}" class="muted">No items found.</td></tr>`;
    return;
  }

  rows.forEach(r => {
    const tr = document.createElement("tr");
    const isFolder = r.item_type === "FOLDER";
    const canOpenFolder = isFolder && VIEW_MODE !== "TRASH";

    const linkHtml = r.web_view_link
      ? `<a class="pill link" href="${r.web_view_link}" target="_blank" rel="noopener">Open</a>`
      : `<span class="pill muted">No link</span>`;

    let actionHtml = `
      <button class="btn mini ghost" data-select="${r.id}">Select</button>
      ${r.web_view_link ? `<button class="btn mini ghost" data-copy="${r.id}">Copy</button>` : ""}
      ${canOpenFolder ? `<button class="btn mini" data-open="${r.id}">Open</button>` : ""}
    `;

    const checkCell = VIEW_MODE === "TRASH"
      ? `<td><input type="checkbox" data-trash-check="${r.id}" ${TRASH_SELECTED.has(r.id) ? "checked" : ""}></td>`
      : "";

    tr.innerHTML = `
      ${checkCell}
      <td><b>${safeText(r.name)}</b>${(VIEW_MODE === "SHARED" && !SHARED_TREE_MODE) ? `<div class="tiny muted">shared item</div>` : ""}</td>
      <td>${safeText(r.item_type)}</td>
      <td>${safeText(r.created_by)}</td>
      <td>${Number(r.admin_locked) === 1 ? `<span class="pill muted">LOCKED</span>` : `<span class="pill">OPEN</span>`}</td>
      <td>${safeText(r.created_at)}</td>
      <td>${safeText(r.deleted_at || "")}</td>
      <td>${linkHtml}</td>
      <td>${actionHtml}</td>
    `;

    tr.style.cursor = "pointer";

    tr.addEventListener("click", (e) => {
      if(e.target.closest("button") || e.target.closest("a") || e.target.closest("input")) return;
      selectItem(r.id);
    });

    tr.addEventListener("dblclick", (e) => {
      if(e.target.closest("button") || e.target.closest("a") || e.target.closest("input")) return;
      if(canOpenFolder){
        openFolderInExplorer(r);
      }
    });

    body.appendChild(tr);

    const selectBtn = tr.querySelector(`[data-select="${r.id}"]`);
    if(selectBtn){
      selectBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        selectItem(r.id);
      });
    }

    const copyBtn = tr.querySelector(`[data-copy="${r.id}"]`);
    if(copyBtn){
      copyBtn.addEventListener("click", async (e) => {
        e.stopPropagation();
        await navigator.clipboard.writeText(r.web_view_link);
        showMsg("listMsg", "Link copied.", true);
      });
    }

    const openBtn = tr.querySelector(`[data-open="${r.id}"]`);
    if(openBtn){
      openBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        openFolderInExplorer(r);
      });
    }

    const trashCheck = tr.querySelector(`[data-trash-check="${r.id}"]`);
    if(trashCheck){
      trashCheck.addEventListener("click", (e) => e.stopPropagation());
      trashCheck.addEventListener("change", () => {
        if(trashCheck.checked) TRASH_SELECTED.add(r.id);
        else TRASH_SELECTED.delete(r.id);
      });
    }
  });
}

async function selectItem(id){
  try{
    const res = await fetch(`/api/docs/items/${id}`, { credentials: "same-origin" });
    const out = await safeJson(res);
    if(!res.ok) throw new Error(out.error || "Failed");

    EDITING_ITEM = out.data.item;
    EDITING_ID = EDITING_ITEM.id;

    $("selectedItemText").textContent = `${EDITING_ITEM.item_type} • ${EDITING_ITEM.name}`;
    $("editName").value = EDITING_ITEM.name || "";
    $("editNotes").value = EDITING_ITEM.notes || "";
    $("editDriveLink").value = EDITING_ITEM.web_view_link || "";
    $("openLinkBtn").href = EDITING_ITEM.web_view_link || "#";

    renderEditPermUsers(out.data.permissions || []);
    setTrashButtons(!!EDITING_ITEM.deleted_at);

    showMsg("editMsg", `Selected item ID ${EDITING_ID}`, true);
  }catch(err){
    showMsg("editMsg", err.message, false);
  }
}

function clearSelection(showMessage = true){
  EDITING_ID = null;
  EDITING_ITEM = null;
  $("selectedItemText").textContent = "No item selected.";
  $("editName").value = "";
  $("editNotes").value = "";
  $("editDriveLink").value = "";
  $("openLinkBtn").href = "#";
  renderEditPermUsers([]);
  setTrashButtons(false);

  if(showMessage){
    showMsg("editMsg", "Selection cleared.", true);
  }else{
    showMsg("editMsg", "", true);
  }
}

async function createFolder(){
  if(creatingFolder) return;

  const btn = $("createFolderBtn");
  const originalText = btn ? btn.textContent : "Create Folder";

  try{
    if(VIEW_MODE === "TRASH") throw new Error("You cannot create folders inside Trash Bin.");

    let parentId = getWorkingParentId();

    if(parentId === null){
      if(getMeRole() === "ADMIN"){
        parentId = "ROOT";
      }else{
        throw new Error("Open or select a folder first.");
      }
    }

    const folderName = ($("folderName").value || "").trim();
    if(!folderName){
      throw new Error("Folder name is required.");
    }

    creatingFolder = true;
    if(btn){
      btn.disabled = true;
      btn.textContent = "Creating...";
    }

    const payload = {
      parent_id: parentId,
      name: folderName,
      notes: ($("folderNotes").value || "").trim(),
      permissions: collectPermissions("create")
    };

    const res = await fetch("/api/docs/folders", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      credentials: "same-origin",
      body: JSON.stringify(payload)
    });

    const out = await safeJson(res);
    if(!res.ok) throw new Error(out.error || "Create folder failed");

    $("folderName").value = "";
    $("folderNotes").value = "";
    renderCreatePermUsers();

    showMsg("folderMsg", "Folder created.", true);
    await loadItems();
    await selectItem(out.data.id);
  }catch(err){
    showMsg("folderMsg", err.message, false);
  }finally{
    creatingFolder = false;
    if(btn){
      btn.disabled = false;
      btn.textContent = originalText;
    }
  }
}

async function uploadDocument(){
  if(uploadingDocument) return;

  const btn = $("uploadBtn");
  const originalText = btn ? btn.textContent : "Upload Document";

  try{
    if(VIEW_MODE === "TRASH") throw new Error("You cannot upload inside Trash Bin.");

    let parentId = getWorkingParentId();

    if(parentId === null){
      if(getMeRole() === "ADMIN"){
        parentId = "ROOT";
      }else{
        throw new Error("Open or select a folder first.");
      }
    }

    const file = $("uploadFile").files[0];
    if(!file) throw new Error("Please choose a file.");

    uploadingDocument = true;
    if(btn){
      btn.disabled = true;
      btn.textContent = "Uploading...";
    }

    const fd = new FormData();
    fd.append("parent_id", String(parentId));
    fd.append("name", ($("uploadDisplayName").value || "").trim());
    fd.append("notes", ($("uploadNotes").value || "").trim());
    fd.append("permissions_json", JSON.stringify(collectPermissions("create")));
    fd.append("file", file);

    const res = await fetch("/api/docs/upload", {
      method: "POST",
      credentials: "same-origin",
      body: fd
    });

    const out = await safeJson(res);
    if(!res.ok) throw new Error(out.error || "Upload failed");

    $("uploadDisplayName").value = "";
    $("uploadNotes").value = "";
    $("uploadFile").value = "";
    renderCreatePermUsers();

    showMsg("uploadMsg", "Document uploaded.", true);
    await loadItems();
    await selectItem(out.data.id);
  }catch(err){
    showMsg("uploadMsg", err.message, false);
  }finally{
    uploadingDocument = false;
    if(btn){
      btn.disabled = false;
      btn.textContent = originalText;
    }
  }
}

async function saveSelectedItem(){
  try{
    if(!EDITING_ID) throw new Error("Select an item first.");
    if(EDITING_ITEM && EDITING_ITEM.deleted_at) throw new Error("Restore item first.");

    const payload = {
      name: ($("editName").value || "").trim(),
      notes: ($("editNotes").value || "").trim(),
      permissions: collectPermissions("edit")
    };

    const res = await fetch(`/api/docs/items/${EDITING_ID}`, {
      method: "PUT",
      headers: {"Content-Type":"application/json"},
      credentials: "same-origin",
      body: JSON.stringify(payload)
    });

    const out = await safeJson(res);
    if(!res.ok) throw new Error(out.error || "Update failed");

    showMsg("editMsg", "Item updated.", true);
    await loadItems();
    await selectItem(EDITING_ID);
  }catch(err){
    showMsg("editMsg", err.message, false);
  }
}

async function deleteSelectedItem(){
  try{
    if(!EDITING_ID) throw new Error("Select an item first.");
    if(!confirm("Move this item to trash?")) return;

    const res = await fetch(`/api/docs/items/${EDITING_ID}`, {
      method: "DELETE",
      credentials: "same-origin"
    });

    const out = await safeJson(res);
    if(!res.ok) throw new Error(out.error || "Delete failed");

    clearSelection();
    showMsg("editMsg", out.warning ? `Moved to trash. ${out.warning}` : "Moved to trash.", true);
    await loadItems();
  }catch(err){
    showMsg("editMsg", err.message, false);
  }
}

async function restoreSelectedItem(){
  try{
    if(!EDITING_ID) throw new Error("Select an item first.");

    const res = await fetch(`/api/docs/items/${EDITING_ID}/restore`, {
      method: "POST",
      credentials: "same-origin"
    });

    const out = await safeJson(res);
    if(!res.ok) throw new Error(out.error || "Restore failed");

    clearSelection();
    showMsg("editMsg", "Item restored.", true);
    await loadItems();
  }catch(err){
    showMsg("editMsg", err.message, false);
  }
}

async function deleteForeverSelectedItem(){
  try{
    if(!EDITING_ID) throw new Error("Select an item first.");
    if(!confirm("Delete forever? This cannot be undone.")) return;

    const res = await fetch(`/api/docs/items/${EDITING_ID}/delete-forever`, {
      method: "POST",
      credentials: "same-origin"
    });

    const out = await safeJson(res);
    if(!res.ok) throw new Error(out.error || "Delete forever failed");

    clearSelection();
    showMsg("editMsg", "Item permanently deleted.", true);
    await loadItems();
  }catch(err){
    showMsg("editMsg", err.message, false);
  }
}

async function toggleLock(){
  try{
    if(!EDITING_ID) throw new Error("Select an item first.");

    const res = await fetch(`/api/docs/items/${EDITING_ID}/toggle-lock`, {
      method: "POST",
      credentials: "same-origin"
    });

    const out = await safeJson(res);
    if(!res.ok) throw new Error(out.error || "Lock toggle failed");

    showMsg("editMsg", Number(out.data.admin_locked) === 1 ? "Admin lock enabled." : "Admin lock removed.", true);
    await loadItems();
    await selectItem(EDITING_ID);
  }catch(err){
    showMsg("editMsg", err.message, false);
  }
}

async function bulkTrashAction(action){
  try{
    if(VIEW_MODE !== "TRASH") throw new Error("Open Trash Bin first.");

    const item_ids = Array.from(TRASH_SELECTED);
    if(!item_ids.length) throw new Error("Select at least one item.");

    const confirmText = action === "restore"
      ? "Restore selected items?"
      : "Delete selected items forever? This cannot be undone.";

    if(!confirm(confirmText)) return;

    const res = await fetch("/api/docs/bulk-trash-action", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      credentials: "same-origin",
      body: JSON.stringify({ action, item_ids })
    });

    const out = await safeJson(res);
    if(!res.ok) throw new Error(out.error || "Bulk action failed");

    TRASH_SELECTED.clear();
    clearSelection(false);
    showMsg("listMsg", `${out.data.updated} item(s) processed.`, true);
    await loadItems();
  }catch(err){
    showMsg("listMsg", err.message, false);
  }
}

function selectAllTrash(){
  CURRENT_ITEMS.forEach(r => TRASH_SELECTED.add(r.id));
  renderItems(CURRENT_ITEMS);
}

function clearAllTrashSelection(){
  TRASH_SELECTED.clear();
  renderItems(CURRENT_ITEMS);
}

function goRoot(){
  VIEW_MODE = "MY";
  CURRENT_PARENT_ID = null;
  PATH_STACK = [];
  TRASH_SELECTED.clear();
  SHARED_TREE_MODE = false;
  clearSelection(false);
  setViewButtons();
  setTrashToolbar();
  loadItems();
}

function goUp(){
  if(VIEW_MODE === "TRASH"){
    goRoot();
    return;
  }

  if(VIEW_MODE === "SHARED" && !SHARED_TREE_MODE){
    goRoot();
    return;
  }

  if(SHARED_TREE_MODE){
    if(PATH_STACK.length <= 1){
      VIEW_MODE = "SHARED";
      SHARED_TREE_MODE = false;
      CURRENT_PARENT_ID = null;
      PATH_STACK = [];
      clearSelection(false);
      setViewButtons();
      setTrashToolbar();
      loadItems();
      return;
    }

    PATH_STACK.pop();
    CURRENT_PARENT_ID = PATH_STACK[PATH_STACK.length - 1].id;
    clearSelection(false);
    loadItems();
    return;
  }

  if(PATH_STACK.length === 0){
    goRoot();
    return;
  }

  PATH_STACK.pop();
  CURRENT_PARENT_ID = PATH_STACK.length ? PATH_STACK[PATH_STACK.length - 1].id : null;
  clearSelection(false);
  loadItems();
}

async function copySelectedLink(){
  const link = $("editDriveLink").value || "";
  if(!link){
    showMsg("editMsg", "No link available.", false);
    return;
  }

  await navigator.clipboard.writeText(link);
  showMsg("editMsg", "Link copied.", true);
}

function switchToShared(){
  VIEW_MODE = "SHARED";
  CURRENT_PARENT_ID = null;
  PATH_STACK = [];
  TRASH_SELECTED.clear();
  SHARED_TREE_MODE = false;
  clearSelection(false);
  setViewButtons();
  setTrashToolbar();
  loadItems();
}

function switchToMyFolders(){
  goRoot();
}

function switchToTrash(){
  VIEW_MODE = "TRASH";
  CURRENT_PARENT_ID = null;
  PATH_STACK = [];
  TRASH_SELECTED.clear();
  SHARED_TREE_MODE = false;
  clearSelection(false);
  setViewButtons();
  setTrashToolbar();
  loadItems();
}

document.addEventListener("DOMContentLoaded", async () => {
  await getMe();
  await loadUsers();
  setViewButtons();
  setTrashToolbar();
  renderPath();

  $("shareSearch").addEventListener("input", renderCreatePermUsers);
  $("listSearch").addEventListener("input", loadItems);

  $("goRootBtn").addEventListener("click", goRoot);
  $("goUpBtn").addEventListener("click", goUp);
  $("refreshDocsBtn").addEventListener("click", loadItems);
  $("myFoldersBtn").addEventListener("click", switchToMyFolders);
  $("sharedBtn").addEventListener("click", switchToShared);

  const trashBtn = $("trashBtn");
  if(trashBtn) trashBtn.addEventListener("click", switchToTrash);

  $("createFolderBtn").addEventListener("click", createFolder);
  $("uploadBtn").addEventListener("click", uploadDocument);
  $("saveEditBtn").addEventListener("click", saveSelectedItem);
  $("deleteItemBtn").addEventListener("click", deleteSelectedItem);

  const restoreItemBtn = $("restoreItemBtn");
  const deleteForeverBtn = $("deleteForeverBtn");
  const clearSelectionBtn = $("clearSelectionBtn");
  const copyLinkBtn = $("copyLinkBtn");

  if(restoreItemBtn) restoreItemBtn.addEventListener("click", restoreSelectedItem);
  if(deleteForeverBtn) deleteForeverBtn.addEventListener("click", deleteForeverSelectedItem);
  if(clearSelectionBtn) clearSelectionBtn.addEventListener("click", () => clearSelection(true));
  if(copyLinkBtn) copyLinkBtn.addEventListener("click", copySelectedLink);

  const toggleLockBtn = $("toggleLockBtn");
  if(toggleLockBtn) toggleLockBtn.addEventListener("click", toggleLock);

  const trashSelectAllBtn = $("trashSelectAllBtn");
  const trashClearAllBtn = $("trashClearAllBtn");
  const trashRestoreSelectedBtn = $("trashRestoreSelectedBtn");
  const trashDeleteSelectedBtn = $("trashDeleteSelectedBtn");

  if(trashSelectAllBtn) trashSelectAllBtn.addEventListener("click", selectAllTrash);
  if(trashClearAllBtn) trashClearAllBtn.addEventListener("click", clearAllTrashSelection);
  if(trashRestoreSelectedBtn) trashRestoreSelectedBtn.addEventListener("click", () => bulkTrashAction("restore"));
  if(trashDeleteSelectedBtn) trashDeleteSelectedBtn.addEventListener("click", () => bulkTrashAction("delete_forever"));

  setTrashButtons(false);
  await loadItems();
});