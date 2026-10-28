/* Server-enforced idle sessions. Only real interaction sends activity updates. */
(() => {
  'use strict';
  const rawFetch = window.fetch.bind(window);
  let state = null, offset = 0, ended = false, dirty = false;
  let lastActivitySent = 0, lastInteraction = 0, checking = false, lastStatus = 0, sending = false;
  const dialog = document.createElement('dialog');
  dialog.className = 'session-dialog';
  dialog.setAttribute('aria-labelledby', 'sessionDialogTitle');
  dialog.innerHTML = '<h2 id="sessionDialogTitle">Still working?</h2><p id="sessionDialogText"></p><p id="sessionDialogError" role="status"></p><div class="session-dialog-actions"><button type="button" class="btn ghost" id="sessionSignOut">Log out</button><button type="button" class="btn" id="sessionStay">Keep me signed in</button><a class="btn" id="sessionLogin" href="/login?reason=expired" hidden>Log in again</a></div>';
  document.body.append(dialog);
  const stay = dialog.querySelector('#sessionStay');
  const logout = dialog.querySelector('#sessionSignOut');
  const login = dialog.querySelector('#sessionLogin');
  const title = dialog.querySelector('#sessionDialogTitle');
  const text = dialog.querySelector('#sessionDialogText');
  const error = dialog.querySelector('#sessionDialogError');
  const show = () => {
    if (!dialog.open) { dialog.showModal(); (!stay.hidden ? stay : login).focus(); }
  };
  const relay = expired => {
    if (!state) return;
    try { localStorage.setItem('erp-session-update', JSON.stringify({state, receivedAt: Date.now(), expired})); } catch (_) {}
  };
  const expire = (broadcast = true) => {
    if (ended) return;
    ended = true;
    title.textContent = 'Please log in again';
    text.textContent = 'Your session has ended for security. Any unsaved changes have not been submitted. Log in again to continue.';
    stay.hidden = true; logout.hidden = true; login.hidden = false; error.textContent = '';
    show(); login.focus();
    if (broadcast) relay(true);
  };
  const apply = (data, receivedAt = Date.now(), broadcast = true) => {
    if (!data.ok || !data.idle_expires_at || !data.absolute_expires_at) return;
    // Do not adopt another account/session into a page with existing user data.
    if (state && data.session_tag !== state.session_tag) { expire(false); return; }
    if (state && data.idle_expires_at < state.idle_expires_at) return;
    state = data; offset = data.server_time * 1000 - receivedAt;
    if (!ended && dialog.open && remaining() > data.warning_seconds) dialog.close();
    if (broadcast) relay(false);
  };
  const remaining = () => state ? Math.ceil(Math.min(state.idle_expires_at, state.absolute_expires_at) - (Date.now() + offset) / 1000) : Infinity;

  // Keep expired responses as responses: never replay failed form submissions.
  window.fetch = async (...args) => {
    const response = await rawFetch(...args);
    const url = new URL(response.url || (args[0] instanceof Request ? args[0].url : args[0]), location.href);
    if (url.origin === location.origin && (response.status === 401 || (response.redirected && url.pathname === '/login'))) expire();
    return response;
  };
  const checkStatus = async () => {
    if (checking || ended) return;
    checking = true;
    try {
      const response = await rawFetch('/api/session/status', {credentials: 'same-origin', cache: 'no-store'});
      lastStatus = Date.now();
      if (response.status === 401 || response.redirected) { expire(); return; }
      if (response.ok) apply(await response.json());
      else if (dialog.open) error.textContent = 'Unable to verify your session. Please try again.';
    } catch (_) { if (dialog.open) error.textContent = 'Connection unavailable. Please try again.'; }
    finally { checking = false; lastStatus = Date.now(); }
  };
  const activity = async (explicit = false) => {
    if (sending || ended || (!explicit && (!dirty || document.hidden))) return;
    if (!explicit && Date.now() - lastInteraction > 60000) { dirty = false; return; }
    sending = true; lastActivitySent = Date.now(); dirty = false;
    if (explicit) { stay.disabled = true; error.textContent = ''; }
    try {
      const response = await rawFetch('/api/session/activity', {
        method: 'POST', credentials: 'same-origin', headers: {'X-ERP-Activity': '1'}
      });
      if (response.status === 401) { expire(); return; }
      if (!response.ok) throw new Error('Session check unavailable');
      apply(await response.json());
      if (!ended && dialog.open) dialog.close();
    } catch (_) {
      dirty = true;
      if (explicit) error.textContent = 'Could not extend your session. Check your connection and try again.';
    } finally { sending = false; stay.disabled = false; }
  };
  const noteActivity = event => {
    if (!event.isTrusted || document.hidden || ended || dialog.open) return;
    dirty = true; lastInteraction = Date.now();
    if (Date.now() - lastActivitySent >= 60000) activity();
  };
  ['pointerdown', 'keydown', 'wheel', 'touchstart', 'mousemove'].forEach(name => {
    document.addEventListener(name, noteActivity, {passive: true});
  });
  stay.addEventListener('click', () => activity(true));
  logout.addEventListener('click', async () => {
    logout.disabled = true;
    try {
      const response = await rawFetch('/api/session/end', {method: 'POST', credentials: 'same-origin', headers: {'X-ERP-Activity': '1'}});
      if (!response.ok && response.status !== 401) throw new Error();
      relay(true); location.assign('/login');
    } catch (_) { logout.disabled = false; error.textContent = 'Could not log out. Check your connection and try again.'; }
  });
  dialog.addEventListener('cancel', event => event.preventDefault());
  window.addEventListener('storage', event => {
    if (event.key !== 'erp-session-update' || !state) return;
    try {
      const update = JSON.parse(event.newValue);
      if (update.state.session_tag !== state.session_tag) return;
      if (update.expired) expire(false);
      else apply(update.state, update.receivedAt, false);
    } catch (_) {}
  });
  const tick = () => {
    if (ended || !state) return;
    const seconds = remaining();
    if (seconds <= 0) { if (Date.now() - lastStatus >= 10000) checkStatus(); return; }
    if (seconds <= state.warning_seconds && !document.hidden) {
      if (Date.now() - lastStatus > 30000) { checkStatus(); return; }
      const minutes = Math.floor(seconds / 60), rest = String(seconds % 60).padStart(2, '0');
      const absolute = state.absolute_expires_at <= state.idle_expires_at;
      title.textContent = absolute ? 'Please finish your work' : 'Still working?';
      text.textContent = absolute ? `Your sign-in needs renewing in ${minutes}:${rest}. Save your work and log in again.` : `You have been inactive. You will be signed out in ${minutes}:${rest}.`;
      stay.hidden = absolute; login.hidden = !absolute; logout.hidden = false;
      show();
    }
  };
  setInterval(() => { if (dirty && Date.now() - lastActivitySent >= 60000) activity(); tick(); }, 1000);
  setInterval(() => { if (!document.hidden) checkStatus(); }, 60000);
  document.addEventListener('visibilitychange', () => { if (!document.hidden) { checkStatus(); tick(); } });
  window.addEventListener('pageshow', event => { if (event.persisted) checkStatus(); });
  checkStatus();
})();
