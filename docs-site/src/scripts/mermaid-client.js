import mermaid from 'mermaid';

function getThemeVariables() {
  const styles = getComputedStyle(document.documentElement);
  const accent = styles.getPropertyValue('--sl-color-accent').trim();
  const bg = styles.getPropertyValue('--sl-color-bg').trim();
  const text = styles.getPropertyValue('--sl-color-text').trim();
  const border = styles.getPropertyValue('--sl-color-hairline-light').trim() || accent;
  const isDark = document.documentElement.dataset.theme === 'dark';

  return {
    darkMode: isDark,
    background: bg,
    primaryColor: bg,
    primaryTextColor: text,
    primaryBorderColor: accent,
    lineColor: accent,
    secondaryColor: bg,
    secondaryTextColor: text,
    secondaryBorderColor: accent,
    tertiaryColor: bg,
    tertiaryTextColor: text,
    tertiaryBorderColor: accent,
    mainBkg: bg,
    nodeBorder: accent,
    clusterBkg: bg,
    clusterBorder: border,
    defaultLinkColor: accent,
    titleColor: text,
    edgeLabelBackground: bg,
    actorBorder: accent,
    actorBkg: bg,
    actorTextColor: text,
    actorLineColor: accent,
    signalColor: accent,
    signalTextColor: text,
    labelBoxBkgColor: bg,
    labelBoxBorderColor: accent,
    labelTextColor: text,
    loopTextColor: text,
    noteBorderColor: border,
    noteBkgColor: bg,
    noteTextColor: text,
    activationBorderColor: accent,
    activationBkgColor: bg,
    sequenceNumberColor: text,
  };
}

async function renderDiagrams() {
  const elements = document.querySelectorAll('pre.mermaid');
  if (!elements || elements.length === 0) return;

  const bodyStyles = getComputedStyle(document.body);
  const bodyFont = bodyStyles.fontFamily || 'inherit';

  mermaid.initialize({
    startOnLoad: false,
    theme: 'base',
    themeVariables: getThemeVariables(),
    securityLevel: 'loose',
    fontFamily: bodyFont,
  });

  const nodes = [];
  for (const el of elements) {
    if (!el.dataset.mermaidSource) {
      el.dataset.mermaidSource = el.textContent;
    } else {
      el.textContent = el.dataset.mermaidSource;
      el.removeAttribute('data-processed');
    }
    nodes.push(el);
  }

  await mermaid.run({ nodes });
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
