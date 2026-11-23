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

function processMarkdownFiles(folder) {
  for (const entry of readdirSync(folder, { withFileTypes: true })) {
    const path = join(folder, entry.name);
    if (entry.isDirectory()) {
      processMarkdownFiles(path);
    } else if (entry.isFile() && entry.name.endsWith('.md')) {
      const content = readFileSync(path, 'utf8');
      const updated = stripDuplicateTitle(content);
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

rmSync(target, { recursive: true, force: true });
mkdirSync(dirname(target), { recursive: true });
cpSync(source, target, { recursive: true });
removeInternal(target);
processMarkdownFiles(target);
publishFailures();
