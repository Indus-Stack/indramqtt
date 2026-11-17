import mermaid from 'mermaid';

function getTheme() {
  const theme = document.documentElement.dataset.theme;
  return theme === 'dark' ? 'dark' : 'default';
}

async function renderDiagrams() {
  const elements = document.querySelectorAll('pre.mermaid');
  if (!elements || elements.length === 0) return;

  mermaid.initialize({
    startOnLoad: false,
    theme: getTheme(),
    securityLevel: 'loose',
    fontFamily: 'inherit'
  });

  await mermaid.run({
    nodes: Array.from(elements)
  });
}

if (typeof document !== 'undefined') {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', renderDiagrams);
  } else {
    renderDiagrams();
  }
  document.addEventListener('astro:page-load', renderDiagrams);

  const observer = new MutationObserver((mutations) => {
    for (const mutation of mutations) {
      if (mutation.type === 'attributes' && mutation.attributeName === 'data-theme') {
        renderDiagrams();
      }
    }
  });
  observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
}
