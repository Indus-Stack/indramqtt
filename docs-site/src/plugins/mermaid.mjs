import DOMPurify from 'dompurify';
import mermaid from 'mermaid';

if (typeof DOMPurify.addHook !== 'function') {
  DOMPurify.addHook = () => {};
  DOMPurify.sanitize = (s) => s;
  DOMPurify.removeAllHooks = () => {};
}

function escapeHtml(str) {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

export function remarkMermaid() {
  return async function transformer(tree, file) {
    const promises = [];
    function visit(node) {
      if (node.type === 'code' && node.lang === 'mermaid') {
        const diagramText = node.value;
        const pagePath = file.history?.[0] || file.path || 'unknown';
        promises.push(
          (async () => {
            try {
              await mermaid.parse(diagramText);
            } catch (err) {
              const msg = `Mermaid syntax error in page ${pagePath}:\n${err.message}\nDiagram:\n${diagramText}`;
              console.error(`\n[ERROR] ${msg}\n`);
              process.exitCode = 1;
              process.exit(1);
            }
            node.type = 'html';
            node.value = `<pre class="mermaid">${escapeHtml(diagramText)}</pre>`;
          })()
        );
      }
      if (node.children && Array.isArray(node.children)) {
        node.children.forEach(visit);
      }
    }
    visit(tree);
    await Promise.all(promises);
  };
}

export default remarkMermaid;
