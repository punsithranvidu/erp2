const password = document.getElementById('password');
const toggle = document.querySelector('.password-toggle');
if (password && toggle) {
  toggle.hidden = false;
  toggle.addEventListener('click', () => {
    const visible = password.type === 'password';
    password.type = visible ? 'text' : 'password';
    toggle.setAttribute('aria-pressed', String(visible));
    toggle.setAttribute('aria-label', visible ? 'Hide password' : 'Show password');
  });
}
const date = document.getElementById('login-date');
if (date) {
  date.textContent = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Colombo', weekday: 'long', year: 'numeric', month: 'long', day: 'numeric'
  }).format(new Date());
}
