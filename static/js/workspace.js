/* Presentation-only navigation; access is still enforced by the server. */
(() => {
  'use strict';
  const body = document.body;
  const employee = body.dataset.appRole !== 'ADMIN';
  window.erpWorkspaceMe = employee
    ? fetch('/api/me', {credentials: 'same-origin'}).then(response => {
        if (!response.ok) throw new Error('Unable to load permissions');
        return response.json();
      })
    : Promise.resolve({role: 'ADMIN'});

  const toggle = document.getElementById('appNavToggle');
  const close = document.getElementById('appNavClose');
  const scrim = document.getElementById('appNavScrim');
  const sidebar = document.getElementById('appSidebar');
  const mobile = window.matchMedia('(max-width: 760px)');
  const setNav = open => {
    body.classList.toggle('app-nav-open', open);
    toggle.setAttribute('aria-expanded', String(open));
    scrim.hidden = !open;
    if (open) close.focus();
    else if (mobile.matches) toggle.focus();
  };
  toggle.addEventListener('click', () => setNav(!body.classList.contains('app-nav-open')));
  close.addEventListener('click', () => setNav(false));
  scrim.addEventListener('click', () => setNav(false));
  mobile.addEventListener('change', () => setNav(false));
  document.addEventListener('keydown', event => {
    if (!body.classList.contains('app-nav-open')) return;
    if (event.key === 'Escape') setNav(false);
    if (event.key === 'Tab') {
      const items = [...sidebar.querySelectorAll('a[href], button')].filter(el => el.getClientRects().length);
      const first = items[0], last = items[items.length - 1];
      if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
      else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
    }
  });

  window.erpWorkspaceMe.then(me => {
    if (!employee) return;
    document.querySelectorAll('[data-nav-module]').forEach(link => {
      const allowed = me.role === 'ADMIN' || Number(me.permissions?.[link.dataset.navModule]?.can_access) === 1;
      link.hidden = !allowed;
    });
    document.querySelectorAll('.app-nav-group').forEach(group => {
      group.hidden = ![...group.querySelectorAll('[data-nav-module]')].some(link => !link.hidden);
    });
  }).catch(() => { document.getElementById('appNavError').hidden = false; });

  const date = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Colombo', day: 'numeric', month: 'short', year: 'numeric'
  }).format(new Date());
  document.querySelectorAll('[data-workspace-date]').forEach(el => { el.textContent = date; });

  const search = document.getElementById('workspaceSearch');
  if (search) {
    const cards = [...document.querySelectorAll('.dash-card')];
    const filter = () => {
      const term = search.value.trim().toLocaleLowerCase();
      let available = 0, shown = 0;
      cards.forEach(card => {
        const permitted = card.style.display !== 'none';
        if (permitted) available++;
        const matches = card.textContent.toLocaleLowerCase().includes(term);
        card.hidden = !permitted || !matches;
        if (!card.hidden) shown++;
      });
      document.getElementById('workspaceModuleCount').textContent = String(available);
      document.getElementById('workspaceNoResults').hidden = !term || shown > 0 || available === 0;
      document.querySelectorAll('.module-section').forEach(section => {
        section.hidden = ![...section.querySelectorAll('.dash-card')].some(card => !card.hidden);
      });
    };
    search.addEventListener('input', filter);
    new MutationObserver(filter).observe(document.getElementById('moduleCards'), {
      subtree: true, attributes: true, attributeFilter: ['style']
    });
    // Employee cards stay hidden until the existing permission loader completes.
    if (!employee) filter();
  }
})();
