// Manage collapsible left sidebar and right TOC panel (DS-02).
// Choices persist across pages via localStorage.

function getStored(key) {
  try {
    return localStorage.getItem(key);
  } catch (_) {
    return null;
  }
}

function setStored(key, val) {
  try {
    localStorage.setItem(key, val);
  } catch (_) {}
}

function updateSidebar(state) {
  const next = state === 'closed' ? 'closed' : 'open';
  document.documentElement.setAttribute('data-sidebar', next);
  const btn = document.getElementById('indra-toggle-sidebar');
  if (btn) {
    btn.setAttribute('aria-expanded', next === 'open' ? 'true' : 'false');
  }
  setStored('indra-docs-sidebar', next);
}

function updateToc(state) {
  const next = state === 'closed' ? 'closed' : 'open';
  document.documentElement.setAttribute('data-toc', next);
  const btn = document.getElementById('indra-toggle-toc');
  if (btn) {
    btn.setAttribute('aria-expanded', next === 'open' ? 'true' : 'false');
  }
  setStored('indra-docs-toc', next);
}

function initPanels() {
  const currentSidebar =
    document.documentElement.getAttribute('data-sidebar') ||
    getStored('indra-docs-sidebar') ||
    'open';
  updateSidebar(currentSidebar);

  const currentToc =
    document.documentElement.getAttribute('data-toc') ||
    getStored('indra-docs-toc') ||
    'open';
  updateToc(currentToc);

  const sidebarBtn = document.getElementById('indra-toggle-sidebar');
  if (sidebarBtn) {
    sidebarBtn.onclick = () => {
      const cur = document.documentElement.getAttribute('data-sidebar') || 'open';
      updateSidebar(cur === 'open' ? 'closed' : 'open');
    };
  }

  const tocBtn = document.getElementById('indra-toggle-toc');
  if (tocBtn) {
    tocBtn.onclick = () => {
      const cur = document.documentElement.getAttribute('data-toc') || 'open';
      updateToc(cur === 'open' ? 'closed' : 'open');
    };
  }
}

// Keyboard shortcut: `[` toggles the left sidebar when focus is not in a text field
function onKeyDown(e) {
  if (e.key !== '[' || e.ctrlKey || e.metaKey || e.altKey) {
    return;
  }
  const btn = document.getElementById('indra-toggle-sidebar');
  if (!btn || window.innerWidth < 1024) {
    return;
  }
  const target = e.target;
  if (
    target instanceof HTMLElement &&
    (target.isContentEditable ||
      target.tagName === 'INPUT' ||
      target.tagName === 'TEXTAREA' ||
      target.tagName === 'SELECT')
  ) {
    return;
  }
  e.preventDefault();
  const cur = document.documentElement.getAttribute('data-sidebar') || 'open';
  updateSidebar(cur === 'open' ? 'closed' : 'open');
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initPanels);
} else {
  initPanels();
}

window.addEventListener('keydown', onKeyDown);
document.addEventListener('astro:page-load', initPanels);
