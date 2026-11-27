// Copies docs/ into the content folder of the site.
//
// - Folders and files whose names start with `_` are internal records of the
//   docs tools. They are not pages, so they stay off the site.
// - One of them is a page that readers need: the list of docs examples that
//   the docs run does not verify. It is published at /reference/verification-failures/.
import { cpSync, existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { FAILURES_ROUTE, FAILURES_SOURCE } from '../remark-md-links.mjs';

const site = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const source = resolve(site, '..', 'docs');
const target = resolve(site, 'src', 'content', 'docs');

function removeInternal(folder) {
  for (const entry of readdirSync(folder, { withFileTypes: true })) {
    const path = join(folder, entry.name);
    if (entry.name.startsWith('_')) {
      rmSync(path, { recursive: true, force: true });
    } else if (entry.isDirectory()) {
      removeInternal(path);
    }
  }
}

// Remove the first Markdown heading line when its text matches the front matter title.
function stripDuplicateTitle(content) {
  const fmMatch = content.match(/^---\r?\n([\s\S]*?)\r?\n---/);
  if (!fmMatch) return content;
  const titleMatch = fmMatch[1].match(/^title:\s*(.*)$/m);
  if (!titleMatch) return content;
  const title = titleMatch[1].trim().replace(/^["']|["']$/g, '');
  const prefix = content.slice(0, fmMatch[0].length);
  const rest = content.slice(fmMatch[0].length);

  const lines = rest.split(/\r?\n/);
  let removed = false;
  const outLines = [];
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!removed && /^#\s+/.test(line)) {
      const heading = line.replace(/^#\s+/, '').trim();
      const norm = (s) =>
        s
          .toLowerCase()
          .replace(/^indramqtt\s+/, '')
          .replace(/\s+layers$/, '')
          .replace(/["']/g, '')
          .trim();
      if (
        heading === title ||
        norm(heading) === norm(title) ||
        heading.toLowerCase().includes(title.toLowerCase())
      ) {
        removed = true;
        if (i + 1 < lines.length && lines[i + 1].trim() === '') {
          i++;
        }
        continue;
      }
    }
    outLines.push(line);
  }
  return prefix + outLines.join('\n');
}

function injectEditUrl(content, relPath) {
  const fmMatch = content.match(/^---\r?\n([\s\S]*?)\r?\n---/);
  if (!fmMatch || /^editUrl:/m.test(fmMatch[1])) return content;
  const editUrl = `https://github.com/ankur-paan/indramqtt/edit/main/docs/${relPath.replace(/\\/g, '/')}`;
  return content.replace(/^---\r?\n/, `---\neditUrl: ${editUrl}\n`);
}

function processMarkdownFiles(folder, root = folder) {
  for (const entry of readdirSync(folder, { withFileTypes: true })) {
    const path = join(folder, entry.name);
    if (entry.isDirectory()) {
      processMarkdownFiles(path, root);
    } else if (entry.isFile() && entry.name.endsWith('.md')) {
      const rel = path.slice(root.length + 1);
      const content = readFileSync(path, 'utf8');
      const updated = injectEditUrl(stripDuplicateTitle(content), rel);
      if (updated !== content) {
        writeFileSync(path, updated, 'utf8');
      }
    }
  }
}

function publishFailures() {
  const page = join(source, FAILURES_SOURCE);
  if (!existsSync(page)) {
    throw new Error(`${page} is missing. The site links to it.`);
  }
  const frontMatter = [
    '---',
    'title: "Verification failures"',
    'description: "The docs examples that the docs run does not verify end to end, with the reason for each."',
    '---',
    '',
    '',
  ].join('\n');
  const out = join(target, `${FAILURES_ROUTE}.md`);
  mkdirSync(dirname(out), { recursive: true });
  const raw = frontMatter + readFileSync(page, 'utf8');
  writeFileSync(out, stripDuplicateTitle(raw), 'utf8');
}

const CATEGORY_NAMES = {
  'relational': 'Relational databases',
  'time-series': 'Time-series databases',
  'document-store': 'Document stores',
  'object-store': 'Storage and analytics',
  'message-broker': 'Message brokers',
  'cloud-messaging': 'Cloud messaging',
  'cloud-iot': 'Cloud IoT',
  'device': 'Industrial devices',
  'http': 'HTTP',
  'local': 'Local',
};

function mapState(rawState) {
  const trimmed = rawState.trim();
  if (trimmed === 'ready') return 'Ready';
  if (trimmed.startsWith('pending')) return 'Pending';
  if (trimmed.startsWith('refused')) return 'Not in this build';
  if (trimmed === 'test-only') return 'test-only';
  throw new Error(`Unknown state: ${trimmed}`);
}

function generateConnectorsJson() {
  const kindsFile = join(source, 'reference', 'connector-kinds.md');
  if (!existsSync(kindsFile)) {
    throw new Error(`${kindsFile} is missing.`);
  }

  const kindsContent = readFileSync(kindsFile, 'utf8');
  const lines = kindsContent.split(/\r?\n/);
  const tableRows = lines.filter(
    (l) => l.startsWith('|') && !l.includes('---') && !l.includes('kind | aliases')
  );

  const kinds = [];
  for (const row of tableRows) {
    const cols = row.split('|').map((c) => c.trim()).slice(1, -1);
    if (cols.length < 5) continue;
    const [kind, aliasesStr, family, _owner, rawState] = cols;
    const aliases = aliasesStr
      ? aliasesStr.split(',').map((a) => a.trim()).filter(Boolean)
      : [];
    kinds.push({
      kind,
      aliases,
      family,
      category: CATEGORY_NAMES[family] || family,
      state: mapState(rawState),
      rawState: rawState.trim(),
    });
  }

  const connectorsDir = join(source, 'connectors');
  const connectorFiles = readdirSync(connectorsDir).filter(
    (f) => f.endsWith('.md') && f !== 'index.md'
  );

  for (const file of connectorFiles) {
    const stem = file.replace(/\.md$/, '');
    const normStem = stem.replace(/-/g, '_');
    const matchingKinds = kinds.filter(
      (k) => k.kind === normStem || k.aliases.includes(normStem)
    );
    if (matchingKinds.length === 0) {
      throw new Error(`Connector page ${file} does not match any kind in connector-kinds.md`);
    }
  }

  const items = [];
  for (const k of kinds) {
    const matchingFiles = connectorFiles.filter((f) => {
      const stem = f.replace(/\.md$/, '');
      const normStem = stem.replace(/-/g, '_');
      return normStem === k.kind || k.aliases.includes(normStem);
    });
    if (matchingFiles.length === 0) {
      throw new Error(`Kind ${k.kind} has no matching page in docs/connectors`);
    }

    if (k.state === 'test-only' || k.rawState === 'test-only') {
      continue;
    }

    let chosenFile;
    const exactMatch = matchingFiles.find((f) => f.replace(/\.md$/, '') === k.kind);
    if (exactMatch) {
      chosenFile = exactMatch;
    } else {
      let maxLines = -1;
      for (const f of matchingFiles) {
        const content = readFileSync(join(connectorsDir, f), 'utf8');
        const lineCount = content.split(/\r?\n/).length;
        if (lineCount > maxLines) {
          maxLines = lineCount;
          chosenFile = f;
        }
      }
    }

    const pageContent = readFileSync(join(connectorsDir, chosenFile), 'utf8');
    const fmMatch = pageContent.match(/^---\r?\n([\s\S]*?)\r?\n---/);
    if (!fmMatch) {
      throw new Error(`Connector page ${chosenFile} is missing front matter`);
    }
    const titleMatch = fmMatch[1].match(/^title:\s*(.*)$/m);
    const descMatch = fmMatch[1].match(/^description:\s*(.*)$/m);
    if (!titleMatch || !descMatch) {
      throw new Error(`Connector page ${chosenFile} is missing title or description in front matter`);
    }
    const title = titleMatch[1].trim().replace(/^["']|["']$/g, '');
    const description = descMatch[1].trim().replace(/^["']|["']$/g, '');
    const page = chosenFile.replace(/\.md$/, '');

    items.push({
      kind: k.kind,
      aliases: k.aliases,
      category: k.category,
      state: k.state,
      title,
      description,
      page,
    });
  }

  const STATE_ORDER = {
    'Ready': 0,
    'Pending': 1,
    'Not in this build': 2,
  };

  items.sort((a, b) => {
    const orderA = STATE_ORDER[a.state] ?? 99;
    const orderB = STATE_ORDER[b.state] ?? 99;
    if (orderA !== orderB) {
      return orderA - orderB;
    }
    return a.title.localeCompare(b.title);
  });

  const dataDir = join(site, 'src', 'data');
  mkdirSync(dataDir, { recursive: true });
  const outFile = join(dataDir, 'connectors.json');
  writeFileSync(outFile, JSON.stringify(items, null, 2) + '\n', 'utf8');
}

rmSync(target, { recursive: true, force: true });
mkdirSync(dirname(target), { recursive: true });
cpSync(source, target, { recursive: true });
removeInternal(target);
processMarkdownFiles(target);
publishFailures();
generateConnectorsJson();
