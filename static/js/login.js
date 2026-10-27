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

const lockoutTimer = document.getElementById('lockout-timer');
if (lockoutTimer) {
  const deadline = Date.now() + Number(lockoutTimer.dataset.seconds) * 1000;
  const updateLockout = () => {
    const remaining = Math.max(0, Math.ceil((deadline - Date.now()) / 1000));
    const minutes = Math.floor(remaining / 60);
    const seconds = String(remaining % 60).padStart(2, '0');
    lockoutTimer.textContent = `${minutes}:${seconds}`;
    if (remaining === 0) {
      clearInterval(lockoutInterval);
      document.querySelector('.login-button').disabled = false;
      const alert = document.querySelector('.login-alert');
      alert.textContent = 'You can try signing in again.';
    }
  };
  const lockoutInterval = setInterval(updateLockout, 1000);
  updateLockout();
}
