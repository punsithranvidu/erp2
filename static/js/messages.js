const $ = (id) => document.getElementById(id);

let ME = null;
let USERS = [];
let CONVERSATIONS = [];
let ACTIVE_CONVO_ID = null;
let ACTIVE_CONVO = null;
let MESSAGES = [];
let GROUP_MEMBERS_CACHE = {};

let SIDEBAR_REFRESH_TIMER = null;
let CHAT_REFRESH_TIMER = null;
let attachedFile = null;

let SENDING = false;
let REPLY_TO_ID = null;
let REPLY_TO_TEXT = "";

let LOADING_CONVERSATIONS = false;
let LOADING_MESSAGES = false;
let LOADING_MEMBERS = false;

const SIDEBAR_REFRESH_MS = 15000;   // notifications / conversation list
const ACTIVE_CHAT_REFRESH_MS = 15000; // only for currently open chat

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

function escapeHtml(str) {
  return String(str || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatDateTime(v) {
  if (!v) return "";
  try {
    const d = new Date(String(v).replace(" ", "T"));
    if (Number.isNaN(d.getTime())) return String(v);
    return d.toLocaleString();
  } catch {
    return String(v);
  }
}

function getMessagesBox() {
  return $("messagesBox");
}

function isNearBottom() {
  const box = getMessagesBox();
  if (!box) return true;
  const distanceFromBottom = box.scrollHeight - box.scrollTop - box.clientHeight;
  return distanceFromBottom < 120;
}

function scrollMessagesToBottom(force = false) {
  const box = getMessagesBox();
  if (!box) return;

  if (force || isNearBottom()) {
    box.scrollTop = box.scrollHeight;
  }

  updateScrollBottomButton();
}

function updateScrollBottomButton() {
  const box = getMessagesBox();
  const btn = $("scrollBottomBtn");
  if (!box || !btn) return;

  const distanceFromBottom = box.scrollHeight - box.scrollTop - box.clientHeight;
  if (distanceFromBottom > 120) {
    btn.classList.add("show");
  } else {
    btn.classList.remove("show");
  }
}

function bindMessageScrollWatcher() {
  const box = getMessagesBox();
  if (!box) return;

  box.addEventListener("scroll", () => {
    updateScrollBottomButton();
  });
}

async function loadMe() {
  ME = await api("/api/me");
}

async function loadUsers() {
  const out = await api("/api/messages/users");
  USERS = out.data || [];
  renderUserPicker();
  renderGroupMembers();
}

function renderUserPicker() {
  const box = $("userPickerBox");
  if (!box) return;

  const q = (($("conversationSearch")?.value) || "").trim().toLowerCase();

  const rows = USERS
    .filter((u) => Number(u.active || 0) === 1)
    .filter((u) => {
      const txt = `${u.username} ${u.full_name || ""} ${u.role || ""}`.toLowerCase();
      return !q || txt.includes(q);
    });

  if (!rows.length) {
    box.innerHTML = `<div class="muted" style="padding:12px;">No users found.</div>`;
    return;
  }

  box.innerHTML = rows.map((u) => `
    <div class="msg-user-row" data-user-start="${u.id}">
      <div>
        <div><b>${escapeHtml(u.full_name || u.username)}</b></div>
        <div class="tiny muted">${escapeHtml(u.username)} • ${escapeHtml(u.role || "")}</div>
      </div>
      <button class="btn mini ghost" type="button">Chat</button>
    </div>
  `).join("");

  box.querySelectorAll("[data-user-start]").forEach((el) => {
    el.addEventListener("click", async () => {
      const userId = Number(el.getAttribute("data-user-start"));
      await startDirectChat(userId);
    });
  });
}

function renderGroupMembers() {
  const box = $("groupMembersBox");
  if (!box) return;

  const rows = USERS.filter((u) => Number(u.active || 0) === 1);

  box.innerHTML = rows.map((u) => `
    <label class="row" style="gap:8px;">
      <input type="checkbox" data-group-user="${u.id}">
      <span>${escapeHtml(u.full_name || u.username)} <span class="tiny muted">(${escapeHtml(u.username)} • ${escapeHtml(u.role || "")})</span></span>
    </label>
  `).join("");
}

async function loadConversations(keepSelection = true, silent = false) {
  if (LOADING_CONVERSATIONS) return;
  LOADING_CONVERSATIONS = true;

  try {
    const out = await api("/api/messages/conversations");
    CONVERSATIONS = out.data || [];

    if (keepSelection && ACTIVE_CONVO_ID) {
      ACTIVE_CONVO =
        CONVERSATIONS.find((x) => Number(x.id) === Number(ACTIVE_CONVO_ID)) || ACTIVE_CONVO;
    } else if (!keepSelection) {
      ACTIVE_CONVO_ID = null;
      ACTIVE_CONVO = null;
    }

    renderConversations();
    renderChatHeader();
  } catch (err) {
    if (!silent) showMsg("leftMsg", err.message, false);
  } finally {
    LOADING_CONVERSATIONS = false;
  }
}

function renderConversations() {
  const box = $("conversationList");
  if (!box) return;

  const q = (($("conversationSearch")?.value) || "").trim().toLowerCase();

  const rows = CONVERSATIONS.filter((c) => {
    const title = c.title || c.other_user_name || c.display_name || "";
    const lastText =
      c.last_message_text ||
      (c.last_message ? c.last_message.message_text : "") ||
      "";
    const txt = `${title} ${lastText}`.toLowerCase();
    return !q || txt.includes(q);
  });

  if (!rows.length) {
    box.innerHTML = `<div class="muted" style="padding:14px;">No conversations yet.</div>`;
    return;
  }

  box.innerHTML = rows.map((c) => {
    const lastText =
      c.last_message_text ||
      (c.last_message ? c.last_message.message_text : "") ||
      "No messages yet";

    const lastAt =
      c.last_message_at ||
      (c.last_message ? c.last_message.created_at : "") ||
      c.created_at ||
      "";

    const title = c.title || c.other_user_name || c.display_name || "Chat";

    return `
      <div class="msg-convo-item ${Number(c.id) === Number(ACTIVE_CONVO_ID) ? "active" : ""}" data-convo-id="${c.id}">
        <div class="msg-convo-main">
          <div class="msg-convo-title">${escapeHtml(title)}</div>
          <div class="msg-convo-snippet">${escapeHtml(lastText)}</div>
          <div class="msg-convo-meta">${escapeHtml(formatDateTime(lastAt))}</div>
        </div>
        <div>
          ${Number(c.unread_count || 0) > 0 ? `<span class="msg-badge">${c.unread_count}</span>` : ``}
        </div>
      </div>
    `;
  }).join("");

  box.querySelectorAll("[data-convo-id]").forEach((el) => {
    el.addEventListener("click", async () => {
      const id = Number(el.getAttribute("data-convo-id"));
      await openConversation(id);
    });
  });
}

async function startDirectChat(userId) {
  try {
    const out = await api("/api/messages/direct", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ user_id: userId })
    });

    ACTIVE_CONVO_ID = Number(out.data.id);
    ACTIVE_CONVO = out.data;

    await loadConversations(true);
    await openConversation(ACTIVE_CONVO_ID);

    showMsg("leftMsg", "Direct chat opened.", true);
  } catch (err) {
    showMsg("leftMsg", err.message, false);
  }
}

async function createGroup() {
  try {
    const title = ($("groupNameInput").value || "").trim();
    if (!title) throw new Error("Group name is required.");

    const memberIds = [];
    document.querySelectorAll("[data-group-user]").forEach((el) => {
      if (el.checked) memberIds.push(Number(el.getAttribute("data-group-user")));
    });

    if (!memberIds.length) throw new Error("Select at least one member.");

    const out = await api("/api/messages/groups", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        title,
        member_user_ids: memberIds
      })
    });

    $("groupNameInput").value = "";
    document.querySelectorAll("[data-group-user]").forEach((el) => {
      el.checked = false;
    });

    $("groupModal").style.display = "none";

    ACTIVE_CONVO_ID = Number(out.data.id);
    ACTIVE_CONVO = out.data;

    await loadConversations(true);
    await openConversation(ACTIVE_CONVO_ID);

    showMsg("leftMsg", "Group created.", true);
    showMsg("groupMsg", "", true);
  } catch (err) {
    showMsg("groupMsg", err.message, false);
  }
}

async function openConversation(conversationId) {
  ACTIVE_CONVO_ID = Number(conversationId);
  ACTIVE_CONVO =
    CONVERSATIONS.find((c) => Number(c.id) === Number(ACTIVE_CONVO_ID)) || ACTIVE_CONVO;

  renderConversations();
  renderChatHeader();

  stopChatRefreshLoop();

  await loadMessages({ forceBottom: true, silent: false });
  await markConversationRead(false);
  await loadConversationMembers(false, false);

  startChatRefreshLoop();
}

function clearActiveConversationUI() {
  ACTIVE_CONVO_ID = null;
  ACTIVE_CONVO = null;
  MESSAGES = [];
  clearReplyPreview();
  clearAttachment();
  stopChatRefreshLoop();
  renderMessages([], { preserveScroll: false, forceBottom: false });
  renderChatHeader();
  renderConversations();
}

function renderChatHeader() {
  const title = $("chatTitle");
  const sub = $("chatSub");
  const addBtn = $("addMemberBtn");
  const leaveBtn = $("leaveGroupBtn");

  if (!title || !sub) return;

  if (!ACTIVE_CONVO) {
    title.textContent = "No conversation selected";
    sub.textContent = "Choose a person or group to start messaging.";
    if (addBtn) addBtn.style.display = "none";
    if (leaveBtn) leaveBtn.style.display = "none";
    return;
  }

  title.textContent =
    ACTIVE_CONVO.title ||
    ACTIVE_CONVO.other_user_name ||
    ACTIVE_CONVO.display_name ||
    "Chat";

  const isGroup = (ACTIVE_CONVO.conversation_type || "").toUpperCase() === "GROUP";

  if (isGroup) {
    const cacheKey = ACTIVE_CONVO_ID;
    const cachedMembers = GROUP_MEMBERS_CACHE[cacheKey] || [];

    if (cachedMembers.length) {
      sub.textContent = cachedMembers
        .map((x) => x.full_name || x.username)
        .join(", ");
    } else if (!LOADING_MEMBERS) {
      sub.textContent = "Group conversation";
    }

    if (addBtn) addBtn.style.display = "";
    if (leaveBtn) leaveBtn.style.display = "";
  } else {
    sub.textContent = ACTIVE_CONVO.other_user_name || "Direct conversation";
    if (addBtn) addBtn.style.display = "none";
    if (leaveBtn) leaveBtn.style.display = "none";
  }
}

async function loadConversationMembers(silent = true, force = false) {
  if (!ACTIVE_CONVO_ID || !ACTIVE_CONVO) return;
  if ((ACTIVE_CONVO.conversation_type || "").toUpperCase() !== "GROUP") return;

  const cacheKey = ACTIVE_CONVO_ID;

  if (!force && GROUP_MEMBERS_CACHE[cacheKey]) {
    const names = GROUP_MEMBERS_CACHE[cacheKey]
      .map((x) => x.full_name || x.username)
      .join(", ");
    $("chatSub").textContent = names || "Group conversation";
    return;
  }

  if (LOADING_MEMBERS) return;

  LOADING_MEMBERS = true;

  try {
    const out = await api(`/api/messages/conversations/${ACTIVE_CONVO_ID}/members`);
    const members = out.data || [];

    GROUP_MEMBERS_CACHE[cacheKey] = members;

    const names = members.map((x) => x.full_name || x.username).join(", ");
    $("chatSub").textContent = names || "Group conversation";
  } catch (err) {
    $("chatSub").textContent = "Group conversation";
    if (!silent) showMsg("rightMsg", err.message, false);
  } finally {
    LOADING_MEMBERS = false;
  }
}

async function loadMessages(opts = {}) {
  if (!ACTIVE_CONVO_ID) {
    renderMessages([], { preserveScroll: false, forceBottom: false });
    return;
  }

  if (LOADING_MESSAGES) return;
  LOADING_MESSAGES = true;

  try {
    const box = getMessagesBox();
    const wasNearBottom = box ? isNearBottom() : true;

    const out = await api(`/api/messages/conversations/${ACTIVE_CONVO_ID}/messages`);
    MESSAGES = out.data || [];

    renderMessages(MESSAGES, {
      preserveScroll: true,
      forceBottom: !!opts.forceBottom,
      wasNearBottom
    });
  } catch (err) {
    if (!opts.silent) showMsg("rightMsg", err.message, false);
  } finally {
    LOADING_MESSAGES = false;
  }
}

function findMessageText(id) {
  const row = MESSAGES.find((x) => Number(x.id) === Number(id));
  if (!row) return "";
  return row.message_text || row.attachment_name || "Attachment";
}

function setReplyPreview(id, text) {
  REPLY_TO_ID = id;
  REPLY_TO_TEXT = text || "";
  $("replyPreviewText").textContent = REPLY_TO_TEXT;
  $("replyPreview").classList.add("show");
}

function clearReplyPreview() {
  REPLY_TO_ID = null;
  REPLY_TO_TEXT = "";
  $("replyPreviewText").textContent = "";
  $("replyPreview").classList.remove("show");
}

function renderMessages(rows, opts = {}) {
  const box = $("messagesBox");
  if (!box) return;

  const previousScrollTop = box.scrollTop;
  const previousScrollHeight = box.scrollHeight;

  if (!rows.length) {
    box.innerHTML = `<div class="empty-chat">No messages yet. Start the conversation.</div>`;
    updateScrollBottomButton();
    return;
  }

  box.innerHTML = rows.map((m) => {
    const mine = m.sender_username === ME.user;
    const who = escapeHtml(m.sender_full_name || m.sender_username || "Unknown");
    const text = escapeHtml(m.message_text || "");
    const attachmentUrl = m.attachment_url || "";
    const attachmentName = escapeHtml(m.attachment_name || "Attachment");
    const canDelete = Number(m.can_delete || 0) === 1;
    const replyText = m.reply_to ? escapeHtml(findMessageText(m.reply_to)) : "";

    return `
      <div class="bubble-wrap ${mine ? "mine" : "other"}">
        <div class="bubble-meta">${who} • ${escapeHtml(formatDateTime(m.created_at || ""))}</div>
        <div class="bubble ${mine ? "mine" : "other"}">
          ${m.reply_to ? `<div class="bubble-reply">${replyText}</div>` : ``}
          ${text ? `<div>${text}</div>` : ``}
          ${attachmentUrl ? `
            <div class="bubble-file">
              📎 <a href="${attachmentUrl}" target="_blank" rel="noopener">${attachmentName}</a>
            </div>
          ` : ``}
        </div>
        <div class="bubble-actions">
          <button type="button" data-reply-msg="${m.id}">Reply</button>
          ${canDelete ? `<button type="button" data-delete-msg="${m.id}">Delete</button>` : ``}
        </div>
      </div>
    `;
  }).join("");

  box.querySelectorAll("[data-delete-msg]").forEach((el) => {
    el.addEventListener("click", async () => {
      const msgId = Number(el.getAttribute("data-delete-msg"));
      await deleteMessage(msgId);
    });
  });

  box.querySelectorAll("[data-reply-msg]").forEach((el) => {
    el.addEventListener("click", () => {
      const msgId = Number(el.getAttribute("data-reply-msg"));
      const txt = findMessageText(msgId);
      setReplyPreview(msgId, txt);
      $("messageInput")?.focus();
    });
  });

  if (opts.forceBottom) {
    box.scrollTop = box.scrollHeight;
  } else if (opts.wasNearBottom) {
    box.scrollTop = box.scrollHeight;
  } else if (opts.preserveScroll) {
    const newScrollHeight = box.scrollHeight;
    const heightDiff = newScrollHeight - previousScrollHeight;
    box.scrollTop = previousScrollTop + heightDiff;
  }

  updateScrollBottomButton();
}

function upsertConversationFromMessage(messageRow) {
  if (!ACTIVE_CONVO_ID || !ACTIVE_CONVO || !messageRow) return;

  ACTIVE_CONVO.last_message = messageRow;
  ACTIVE_CONVO.last_message_text =
    messageRow.message_text || messageRow.attachment_name || "Attachment";
  ACTIVE_CONVO.last_message_at = messageRow.created_at || "";
  ACTIVE_CONVO.unread_count = 0;

  const idx = CONVERSATIONS.findIndex((c) => Number(c.id) === Number(ACTIVE_CONVO_ID));
  if (idx >= 0) {
    CONVERSATIONS[idx] = {
      ...CONVERSATIONS[idx],
      last_message: messageRow,
      last_message_text: messageRow.message_text || messageRow.attachment_name || "Attachment",
      last_message_at: messageRow.created_at || "",
      unread_count: 0
    };

    const updated = CONVERSATIONS.splice(idx, 1)[0];
    CONVERSATIONS.unshift(updated);
  } else {
    CONVERSATIONS.unshift({
      ...ACTIVE_CONVO,
      last_message: messageRow,
      last_message_text: messageRow.message_text || messageRow.attachment_name || "Attachment",
      last_message_at: messageRow.created_at || "",
      unread_count: 0
    });
  }
}

function renderAttachedFile() {
  const wrap = $("attachedFileName");
  if (!wrap) return;

  if (!attachedFile) {
    wrap.innerHTML = "";
    return;
  }

  wrap.innerHTML = `
    <span class="attached-file-pill">
      <span class="attached-file-name">${escapeHtml(attachedFile.name)}</span>
      <button type="button" id="removeFileBtn" class="attached-file-remove" title="Remove file">✕</button>
    </span>
  `;

  $("removeFileBtn")?.addEventListener("click", clearAttachment);
}

function clearAttachment() {
  attachedFile = null;
  if ($("messageFileInput")) $("messageFileInput").value = "";
  if ($("attachedFileName")) $("attachedFileName").innerHTML = "";
}

async function sendMessage() {
  try {
    if (SENDING) return;
    if (!ACTIVE_CONVO_ID) throw new Error("Select a conversation first.");

    const text = ($("messageInput")?.value || "").trim();
    const file = attachedFile || $("messageFileInput")?.files?.[0] || null;

    if (!text && !file) {
      throw new Error("Type a message or attach a file.");
    }

    SENDING = true;

    const btn = $("sendMessageBtn");
    if (btn) {
      btn.disabled = true;
      btn.textContent = "Sending...";
    }

    const fd = new FormData();
    fd.append("message_text", text);
    if (file) fd.append("file", file);
    if (REPLY_TO_ID) fd.append("reply_to", String(REPLY_TO_ID));

    const currentConvoId = ACTIVE_CONVO_ID;

    const res = await fetch(`/api/messages/conversations/${currentConvoId}/messages`, {
      method: "POST",
      credentials: "same-origin",
      body: fd
    });

    const out = await safeJson(res);
    if (!res.ok) throw new Error(out.error || "Send failed");

    $("messageInput").value = "";
    clearAttachment();
    clearReplyPreview();

    const newMsg = out.data || null;

    if (newMsg && Number(currentConvoId) === Number(ACTIVE_CONVO_ID)) {
      MESSAGES.push(newMsg);
      renderMessages(MESSAGES, {
        forceBottom: true,
        preserveScroll: false,
        wasNearBottom: true
      });
      upsertConversationFromMessage(newMsg);
      renderConversations();
      renderChatHeader();
      scrollMessagesToBottom(true);
    } else {
      await loadMessages({ forceBottom: true, silent: false });
      await loadConversations(true, true);
    }

    showMsg("rightMsg", "Message sent.", true);
  } catch (err) {
    showMsg("rightMsg", err.message, false);
  } finally {
    SENDING = false;
    const btn = $("sendMessageBtn");
    if (btn) {
      btn.disabled = false;
      btn.textContent = "Send";
    }
  }
}

async function deleteMessage(messageId) {
  try {
    if (!confirm("Delete this message?")) return;

    await api(`/api/messages/messages/${messageId}`, {
      method: "DELETE"
    });

    await loadMessages({ silent: false });
    await loadConversations(true, true);
    showMsg("rightMsg", "Message deleted.", true);
  } catch (err) {
    showMsg("rightMsg", err.message, false);
  }
}

async function deleteConversation() {
  try {
    if (!ACTIVE_CONVO_ID) throw new Error("Select a conversation first.");
    if (!confirm("Delete this conversation to trash?")) return;

    await api(`/api/messages/conversations/${ACTIVE_CONVO_ID}/delete`, {
      method: "POST"
    });

    clearActiveConversationUI();

    await loadConversations(false, false);

    showMsg("leftMsg", "Conversation moved to trash.", true);
  } catch (err) {
    showMsg("leftMsg", err.message, false);
  }
}

async function leaveGroup() {
  try {
    if (!ACTIVE_CONVO_ID) throw new Error("Select a conversation first.");
    if (!ACTIVE_CONVO || (ACTIVE_CONVO.conversation_type || "").toUpperCase() !== "GROUP") {
      throw new Error("This is not a group.");
    }
    if (!confirm("Leave this group?")) return;

    delete GROUP_MEMBERS_CACHE[ACTIVE_CONVO_ID];

    await api(`/api/messages/groups/${ACTIVE_CONVO_ID}/leave`, {
      method: "POST"
    });

    clearActiveConversationUI();

    await loadConversations(false, false);

    showMsg("leftMsg", "You left the group.", true);
  } catch (err) {
    showMsg("leftMsg", err.message, false);
  }
}

async function openAddMemberModal() {
  try {
    if (!ACTIVE_CONVO_ID || !ACTIVE_CONVO) throw new Error("Select a group first.");
    if ((ACTIVE_CONVO.conversation_type || "").toUpperCase() !== "GROUP") {
      throw new Error("Add Member works only for group chats.");
    }

    const membersOut = await api(`/api/messages/conversations/${ACTIVE_CONVO_ID}/members`);
    const memberIds = new Set((membersOut.data || []).map((x) => Number(x.id)));

    const available = USERS.filter(
      (u) => Number(u.active || 0) === 1 && !memberIds.has(Number(u.id))
    );
    const box = $("addMemberBox");

    if (!available.length) {
      box.innerHTML = `<div class="muted" style="padding:12px;">No more users available to add.</div>`;
    } else {
      box.innerHTML = available.map((u) => `
        <label class="row" style="gap:8px;">
          <input type="checkbox" data-add-member-user="${u.id}">
          <span>${escapeHtml(u.full_name || u.username)} <span class="tiny muted">(${escapeHtml(u.username)} • ${escapeHtml(u.role || "")})</span></span>
        </label>
      `).join("");
    }

    $("addMemberModal").style.display = "block";
    showMsg("addMemberMsg", "", true);
  } catch (err) {
    showMsg("leftMsg", err.message, false);
  }
}

async function addMembersToGroup() {
  try {
    if (!ACTIVE_CONVO_ID) throw new Error("Select a group first.");

    const ids = [];
    document.querySelectorAll("[data-add-member-user]").forEach((el) => {
      if (el.checked) ids.push(Number(el.getAttribute("data-add-member-user")));
    });

    if (!ids.length) throw new Error("Select at least one member.");

    await api(`/api/messages/groups/${ACTIVE_CONVO_ID}/add-members`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ member_user_ids: ids })
    });

    delete GROUP_MEMBERS_CACHE[ACTIVE_CONVO_ID];

    $("addMemberModal").style.display = "none";
    await loadConversationMembers(false, true);
    await loadConversations(true, true);
    showMsg("leftMsg", "Members added successfully.", true);
  } catch (err) {
    showMsg("addMemberMsg", err.message, false);
  }
}

async function loadTrash() {
  try {
    const out = await api("/api/messages/trash");
    const rows = out.data || [];
    const box = $("trashList");

    if (!rows.length) {
      box.innerHTML = `<div class="muted" style="padding:12px;">Trash is empty.</div>`;
      return;
    }

    box.innerHTML = rows.map((r) => `
      <div class="trash-item">
        <div><b>${escapeHtml(r.title || r.other_user_name || "Chat")}</b></div>
        <div class="tiny muted">${escapeHtml(formatDateTime(r.deleted_at || r.user_deleted_at || ""))}</div>
        <div class="row" style="margin-top:8px; gap:8px;">
          <button class="btn ghost mini" type="button" data-restore-chat="${r.id}">Restore</button>
          ${ME.role === "ADMIN" ? `<button class="btn mini danger" type="button" data-permanent-chat="${r.id}">Delete Permanently</button>` : ``}
        </div>
      </div>
    `).join("");

    box.querySelectorAll("[data-restore-chat]").forEach((el) => {
      el.addEventListener("click", async () => {
        await restoreConversation(Number(el.getAttribute("data-restore-chat")));
      });
    });

    box.querySelectorAll("[data-permanent-chat]").forEach((el) => {
      el.addEventListener("click", async () => {
        await permanentlyDeleteConversation(Number(el.getAttribute("data-permanent-chat")));
      });
    });
  } catch (err) {
    showMsg("trashMsg", err.message, false);
  }
}

async function restoreConversation(conversationId) {
  try {
    await api(`/api/messages/conversations/${conversationId}/restore`, { method: "POST" });
    await loadTrash();
    await loadConversations(false, true);
    showMsg("trashMsg", "Conversation restored.", true);
  } catch (err) {
    showMsg("trashMsg", err.message, false);
  }
}

async function permanentlyDeleteConversation(conversationId) {
  try {
    if (!confirm("Delete this chat permanently?")) return;
    await api(`/api/messages/conversations/${conversationId}/permanent-delete`, {
      method: "POST"
    });
    await loadTrash();
    await loadConversations(false, true);
    showMsg("trashMsg", "Conversation permanently deleted.", true);
  } catch (err) {
    showMsg("trashMsg", err.message, false);
  }
}

async function markConversationRead(refreshList = true) {
  if (!ACTIVE_CONVO_ID) return;

  try {
    await api(`/api/messages/conversations/${ACTIVE_CONVO_ID}/read`, {
      method: "POST"
    });

    if (refreshList) {
      await loadConversations(true, true);
      ACTIVE_CONVO =
        CONVERSATIONS.find((c) => Number(c.id) === Number(ACTIVE_CONVO_ID)) || ACTIVE_CONVO;

      renderConversations();
      renderChatHeader();
    } else {
      const idx = CONVERSATIONS.findIndex((c) => Number(c.id) === Number(ACTIVE_CONVO_ID));
      if (idx >= 0) {
        CONVERSATIONS[idx].unread_count = 0;
      }
      if (ACTIVE_CONVO) {
        ACTIVE_CONVO.unread_count = 0;
      }
      renderConversations();
    }
  } catch (err) {
    console.error(err);
  }
}

function toggleEmojiBox() {
  const box = $("emojiBox");
  if (box) box.classList.toggle("open");
}

function bindEmojiButtons() {
  document.querySelectorAll(".emoji-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const input = $("messageInput");
      if (!input) return;
      input.value += btn.textContent;
      input.focus();
    });
  });
}

async function refreshSidebarOnce() {
  if (document.hidden) return;
  await loadConversations(true, true);
}

async function refreshActiveChatOnce() {
  if (document.hidden) return;
  if (!ACTIVE_CONVO_ID) return;

  const wasNearBottom = isNearBottom();
  await loadMessages({ forceBottom: false, silent: true });
  await markConversationRead(false);

  if (wasNearBottom) {
    scrollMessagesToBottom(true);
  } else {
    updateScrollBottomButton();
  }
}

function stopSidebarRefreshLoop() {
  if (SIDEBAR_REFRESH_TIMER) {
    clearInterval(SIDEBAR_REFRESH_TIMER);
    SIDEBAR_REFRESH_TIMER = null;
  }
}

function startSidebarRefreshLoop() {
  stopSidebarRefreshLoop();

  SIDEBAR_REFRESH_TIMER = setInterval(async () => {
    await refreshSidebarOnce();
  }, SIDEBAR_REFRESH_MS);
}

function stopChatRefreshLoop() {
  if (CHAT_REFRESH_TIMER) {
    clearInterval(CHAT_REFRESH_TIMER);
    CHAT_REFRESH_TIMER = null;
  }
}

function startChatRefreshLoop() {
  stopChatRefreshLoop();

  if (!ACTIVE_CONVO_ID) return;

  CHAT_REFRESH_TIMER = setInterval(async () => {
    await refreshActiveChatOnce();
  }, ACTIVE_CHAT_REFRESH_MS);
}

function stopAllRefreshLoops() {
  stopSidebarRefreshLoop();
  stopChatRefreshLoop();
}

document.addEventListener("DOMContentLoaded", async () => {
  bindEmojiButtons();
  bindMessageScrollWatcher();

  await loadMe();
  await loadUsers();
  await loadConversations(false, false);

  startSidebarRefreshLoop();

  $("conversationSearch")?.addEventListener("input", () => {
    renderUserPicker();
    renderConversations();
  });

  $("refreshConvosBtn")?.addEventListener("click", async () => {
    await loadConversations(true, false);
    if (ACTIVE_CONVO_ID) {
      await loadMessages({ silent: false });
    }
  });

  $("newGroupBtn")?.addEventListener("click", () => {
    $("groupModal").style.display = "block";
    showMsg("groupMsg", "", true);
  });

  $("closeGroupModalBtn")?.addEventListener("click", () => {
    $("groupModal").style.display = "none";
  });

  $("createGroupSubmitBtn")?.addEventListener("click", createGroup);
  $("sendMessageBtn")?.addEventListener("click", sendMessage);
  $("deleteConversationBtn")?.addEventListener("click", deleteConversation);
  $("leaveGroupBtn")?.addEventListener("click", leaveGroup);
  $("clearReplyBtn")?.addEventListener("click", clearReplyPreview);

  $("addMemberBtn")?.addEventListener("click", openAddMemberModal);
  $("closeAddMemberModalBtn")?.addEventListener("click", () => {
    $("addMemberModal").style.display = "none";
  });
  $("addMemberSubmitBtn")?.addEventListener("click", addMembersToGroup);

  $("openTrashBtn")?.addEventListener("click", async () => {
    $("trashModal").style.display = "block";
    await loadTrash();
  });

  $("closeTrashModalBtn")?.addEventListener("click", () => {
    $("trashModal").style.display = "none";
  });

  $("messageInput")?.addEventListener("keydown", async (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      await sendMessage();
    }
  });

  $("emojiToggleBtn")?.addEventListener("click", toggleEmojiBox);

  $("messageFileInput")?.addEventListener("change", () => {
    attachedFile = $("messageFileInput").files[0] || null;
    renderAttachedFile();
  });

  $("markReadBtn")?.addEventListener("click", () => markConversationRead(true));

  $("scrollBottomBtn")?.addEventListener("click", () => {
    scrollMessagesToBottom(true);
  });

  document.addEventListener("visibilitychange", async () => {
    if (document.hidden) {
      stopAllRefreshLoops();
    } else {
      startSidebarRefreshLoop();

      await refreshSidebarOnce();

      if (ACTIVE_CONVO_ID) {
        await refreshActiveChatOnce();
        startChatRefreshLoop();
      }
    }
  });
});

window.addEventListener("beforeunload", stopAllRefreshLoops);