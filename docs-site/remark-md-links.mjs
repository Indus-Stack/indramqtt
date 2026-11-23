// Rewrites the links between docs pages for the published site.
//
// The files in docs/ link to each other by file name (`./sql.md`), so that
// GitHub shows the links. The site serves `docs/rules/sql.md` at
// `/rules/sql/`. This plugin turns the first form into the second while the
// site builds. The source files stay as they are.
import fs from 'node:fs';
import path from 'node:path';

// Where a link to a file outside docs/ goes.
export const REPOSITORY_BLOB_URL = 'https://github.com/ankur-paan/indramqtt/blob/main';

// The page of the failed docs examples lives in an internal folder. The
// site publishes it under a clean route (see scripts/sync-docs.mjs).
export const FAILURES_SOURCE = '_verification/failures.md';
export const FAILURES_ROUTE = 'reference/verification-failures';

const SCHEME = /^[a-z][a-z0-9+.-]*:/i;

/**
 * Returns the site URL for one link, or the link itself when it needs no
 * change or when its target does not exist (a link checker then reports it).
 *
 * @param {string} url The link as written in the Markdown file.
 * @param {object} context
 * @param {string} context.fromDir Folder of the file that holds the link.
 * @param {string} context.docsRoot Folder of the docs pages in the site.
 * @param {string} context.repoRoot Root folder of the repository.
 * @param {(file: string) => boolean} [context.exists] Test for a file.
 */
export function rewriteLink(url, { fromDir, docsRoot, repoRoot, exists = fs.existsSync }) {
  const [, file, tail = ''] = /^([^?#]*)([?#].*)?$/.exec(url);
  if (!file || SCHEME.test(file) || file.startsWith('/') || !/\.mdx?$/.test(file)) {
    return url;
  }
  const target = path.resolve(fromDir, file);
  const relative = path.relative(docsRoot, target).split(path.sep).join('/');

  if (relative === '..' || relative.startsWith('../')) {
    // The file is outside docs/. The docs folder sits at `docs/` in the
    // repository, so the same relative path names the repository file.
    const repoPath = path.posix.normalize(path.posix.join('docs', relative));
    if (repoPath.startsWith('..') || !exists(path.join(repoRoot, repoPath))) {
      return url;
    }
    return `${REPOSITORY_BLOB_URL}/${repoPath}${tail}`;
  }
  if (relative === FAILURES_SOURCE) {
    return `/${FAILURES_ROUTE}/${tail}`;
  }
  if (relative.split('/').some((part) => part.startsWith('_')) || !exists(target)) {
    return url;
  }
  let slug = relative.replace(/\.mdx?$/, '');
  if (slug === 'index') {
    slug = '';
  } else if (slug.endsWith('/index')) {
    slug = slug.slice(0, -'/index'.length);
  }
  return `/${slug}${slug ? '/' : ''}${tail}`;
}

function walk(node, visit) {
  visit(node);
  if (Array.isArray(node.children)) {
    for (const child of node.children) {
      walk(child, visit);
    }
  }
}

/** Remark plugin. Options: `docsRoot` (default `src/content/docs`) and `repoRoot`. */
export default function remarkMdLinks(options = {}) {
  const docsRoot = path.resolve(options.docsRoot ?? 'src/content/docs');
  const repoRoot = path.resolve(options.repoRoot ?? path.join(docsRoot, '..', '..', '..', '..'));
  return (tree, file) => {
    if (!file || !file.path) {
      return;
    }
    const fromDir = path.dirname(path.resolve(file.path));
    walk(tree, (node) => {
      if ((node.type === 'link' || node.type === 'definition') && typeof node.url === 'string') {
        node.url = rewriteLink(node.url, { fromDir, docsRoot, repoRoot });
      }
    });
  };
}
