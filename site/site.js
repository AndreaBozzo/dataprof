/* Enhance the language examples and announce clipboard results separately. */
const tabs = [...document.querySelectorAll('.tab')];
function selectTab(tab) {
  for (const candidate of tabs) {
    const selected = candidate === tab;
    candidate.setAttribute('aria-selected', String(selected));
    candidate.tabIndex = selected ? 0 : -1;
    document.getElementById(candidate.getAttribute('aria-controls')).hidden = !selected;
  }
}
// Select a tab and record it in the URL hash (e.g. #tab-rust) without scrolling.
function activateTab(tab) {
  selectTab(tab);
  history.replaceState(null, '', `#${tab.id}`);
}
// Open the tab named by the hash on load; leave the hash alone otherwise.
const hashTab = tabs.find((tab) => tab.id === location.hash.slice(1));
if (hashTab) selectTab(hashTab);
for (const tab of tabs) {
  tab.addEventListener('click', () => activateTab(tab));
  tab.addEventListener('keydown', (event) => {
    if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
    event.preventDefault();
    const index = tabs.indexOf(tab);
    const offset = event.key === 'ArrowLeft' ? -1 : 1;
    const next = event.key === 'Home' ? tabs[0]
      : event.key === 'End' ? tabs.at(-1)
      : tabs[(index + offset + tabs.length) % tabs.length];
    activateTab(next);
    next.focus();
  });
}

const status = document.getElementById('copy-status');
let clearStatus;
for (const button of document.querySelectorAll('[data-copy]')) {
  button.addEventListener('click', async () => {
    const source = document.getElementById(button.dataset.copy);
    clearTimeout(clearStatus);
    status.textContent = '';
    try {
      await navigator.clipboard.writeText(source.textContent);
      status.textContent = `${button.getAttribute('aria-label').replace(/^Copy /, '')} copied.`;
    } catch {
      const range = document.createRange();
      range.selectNodeContents(source);
      const selection = window.getSelection();
      selection.removeAllRanges();
      selection.addRange(range);
      status.textContent = 'Clipboard unavailable. Text selected; press Ctrl+C or Command+C to copy.';
    }
    clearStatus = setTimeout(() => { status.textContent = ''; }, 6000);
  });
}
