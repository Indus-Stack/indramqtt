// Run with `npm test` (the built-in Node test runner, no dependency).
import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';
import remarkMdLinks, { REPOSITORY_BLOB_URL, rewriteLink } from '../remark-md-links.mjs';

let repoRoot;
let docsRoot;

function touch(file) {
  mkdirSync(path.dirname(file), { recursive: true });
  writeFileSync(file, '# page\n');
}

// A small copy of the layout that sync-docs makes:
// <repo>/docs-site/src/content/docs/... and <repo>/benchmark/README.md.
before(() => {
  repoRoot = mkdtempSync(path.join(tmpdir(), 'md-links-'));
  docsRoot = path.join(repoRoot, 'docs-site', 'src', 'content', 'docs');
  for (const page of [
    'index.md',
    'rules/index.md',
    'rules/sql.md',
    'rules/functions.md',
    'concepts/index.md',
    'connectors/index.md',
    'connectors/aws_iot.md',
    'operations/sizing.md',
  ]) {
    touch(path.join(docsRoot, page));
  }
  touch(path.join(repoRoot, 'benchmark', 'README.md'));
});

after(() => rmSync(repoRoot, { recursive: true, force: true }));

function rewrite(fromFile, url) {
  return rewriteLink(url, {
    fromDir: path.dirname(path.join(docsRoot, fromFile)),
    docsRoot,
    repoRoot,
  });
}

describe('rewriteLink', () => {
  it('turns a link to a file in the same folder into the page URL', () => {
    assert.equal(rewrite('rules/index.md', './sql.md'), '/rules/sql/');
    assert.equal(rewrite('rules/index.md', 'functions.md'), '/rules/functions/');
  });

  it('follows a link to a parent folder', () => {
    assert.equal(rewrite('rules/sql.md', '../operations/sizing.md'), '/operations/sizing/');
  });

  it('maps an index.md to the folder URL', () => {
    assert.equal(rewrite('index.md', './concepts/index.md'), '/concepts/');
    assert.equal(rewrite('rules/sql.md', './index.md'), '/rules/');
    assert.equal(rewrite('rules/index.md', '../index.md'), '/');
  });

  it('keeps an anchor and a query', () => {
    assert.equal(rewrite('rules/index.md', './sql.md#where'), '/rules/sql/#where');
    assert.equal(rewrite('rules/index.md', './sql.md?x=1#where'), '/rules/sql/?x=1#where');
  });

  it('keeps an underscore in a file name', () => {
    assert.equal(rewrite('connectors/index.md', './aws_iot.md'), '/connectors/aws_iot/');
  });

  it('turns a link to a repository file outside docs/ into a repository URL', () => {
    assert.equal(
      rewrite('operations/sizing.md', '../../benchmark/README.md'),
      `${REPOSITORY_BLOB_URL}/benchmark/README.md`,
    );
  });

  it('publishes the failures page under its own route', () => {
    assert.equal(
      rewrite('connectors/aws_iot.md', '../_verification/failures.md'),
      '/reference/verification-failures/',
    );
    assert.equal(
      rewrite('connectors/aws_iot.md', '../_verification/failures.md#alloydb'),
      '/reference/verification-failures/#alloydb',
    );
  });

  it('leaves every other link into an internal folder as it is', () => {
    assert.equal(rewrite('index.md', './_verification/captured-outputs.md'), './_verification/captured-outputs.md');
  });

  it('leaves an absolute URL, a path from the root and a non-Markdown link as they are', () => {
    assert.equal(rewrite('index.md', 'https://example.com/a.md'), 'https://example.com/a.md');
    assert.equal(rewrite('index.md', 'mailto:someone@example.com'), 'mailto:someone@example.com');
    assert.equal(rewrite('index.md', '/rules/sql/'), '/rules/sql/');
    assert.equal(rewrite('index.md', './rules/'), './rules/');
    assert.equal(rewrite('index.md', '#top'), '#top');
  });

  it('leaves a link to a file that does not exist as it is', () => {
    assert.equal(rewrite('rules/index.md', './missing.md'), './missing.md');
    assert.equal(rewrite('operations/sizing.md', '../../benchmark/missing.md'), '../../benchmark/missing.md');
    assert.equal(rewrite('operations/sizing.md', '../../../outside.md'), '../../../outside.md');
  });
});

describe('remarkMdLinks plugin', () => {
  it('changes links and link definitions, and does not touch code', () => {
    const tree = {
      type: 'root',
      children: [
        {
          type: 'paragraph',
          children: [
            { type: 'link', url: './sql.md', children: [{ type: 'text', value: 'SQL' }] },
            { type: 'inlineCode', value: '[x](./sql.md)' },
          ],
        },
        { type: 'code', value: '[x](./sql.md)' },
        { type: 'definition', identifier: 'f', url: './functions.md' },
      ],
    };
    const transform = remarkMdLinks({ docsRoot, repoRoot });
    transform(tree, { path: path.join(docsRoot, 'rules', 'index.md') });
    assert.equal(tree.children[0].children[0].url, '/rules/sql/');
    assert.equal(tree.children[0].children[1].value, '[x](./sql.md)');
    assert.equal(tree.children[1].value, '[x](./sql.md)');
    assert.equal(tree.children[2].url, '/rules/functions/');
  });

  it('does nothing when the file has no path', () => {
    const tree = { type: 'root', children: [{ type: 'link', url: './sql.md', children: [] }] };
    remarkMdLinks({ docsRoot, repoRoot })(tree, {});
    assert.equal(tree.children[0].url, './sql.md');
  });
});
