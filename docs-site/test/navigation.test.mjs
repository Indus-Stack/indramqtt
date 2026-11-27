// Test sidebar navigation and redirects (DS-04, R-NOFAKE).
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { describe, it } from 'node:test';

const distDir = path.resolve('dist');

function parseSidebar(html) {
  const navMatch =
    html.match(/<nav[^>]*aria-label="Main"[^>]*>([\s\S]*?)<\/nav>/) ||
    html.match(/<div[^>]*id="starlight__sidebar"[^>]*>([\s\S]*?)<\/div>\s*<\/div>/);
  assert.ok(navMatch, 'sidebar navigation must exist in HTML');
  const navHtml = navMatch[0];

  const openGroups = [];
  const closedGroups = [];
  const visibleLinks = [];

  // Match each details group and its summary
  const detailsMatches = [...navHtml.matchAll(/<details\b([^>]*)>([\s\S]*?)<\/details>/gi)];
  for (const dm of detailsMatches) {
    const attrs = dm[1];
    const inner = dm[2];
    const summaryMatch = inner.match(/<summary\b[^>]*>([\s\S]*?)<\/summary>/i);
    const label = summaryMatch ? summaryMatch[1].replace(/<[^>]+>/g, '').trim() : '';
    if (/\bopen\b/i.test(attrs)) {
      openGroups.push(label);
    } else {
      closedGroups.push(label);
    }
  }

  // Count visible links (links where every ancestor details has 'open')
  const tagRegex = /<(\/)?(details|a)\b([^>]*)>/gi;
  let match;
  const detailsStack = [];
  while ((match = tagRegex.exec(navHtml)) !== null) {
    const isClosing = match[1] === '/';
    const tag = match[2].toLowerCase();
    const attrs = match[3];

    if (tag === 'details') {
      if (isClosing) {
        detailsStack.pop();
      } else {
        const isOpen = /\bopen\b/i.test(attrs);
        detailsStack.push(isOpen);
      }
    } else if (tag === 'a' && !isClosing) {
      const isVisible = detailsStack.every((open) => open);
      if (isVisible) {
        const hrefMatch = attrs.match(/href="([^"]*)"/i);
        const href = hrefMatch ? hrefMatch[1] : '';
        visibleLinks.push(href);
      }
    }
  }

  return { openGroups, closedGroups, visibleLinks };
}

function parseRedirectsFromConfig() {
  const configContent = readFileSync(path.resolve('astro.config.mjs'), 'utf8');
  const redirectsMatch = configContent.match(/redirects:\s*\{([\s\S]*?)\},/);
  assert.ok(redirectsMatch, 'redirects block must exist in astro.config.mjs');
  const lines = redirectsMatch[1].split('\n');
  const redirects = {};
  for (const line of lines) {
    const m = line.match(/['"]([^'"]+)['"]\s*:\s*['"]([^'"]+)['"]/);
    if (m) {
      redirects[m[1]] = m[2];
    }
  }
  return redirects;
}

describe('navigation and redirects tests (DS-04)', () => {
  it('fails when dist is absent (R-NOFAKE)', () => {
    assert.ok(
      existsSync(distDir),
      'dist directory must exist. Run npm run build first.'
    );
  });

  const testPages = [
    {
      path: '/getting-started/install/',
      expectedOpenGroup: 'Get started',
    },
    {
      path: '/connectors/kafka/',
      expectedOpenGroup: 'Connectors',
      expectedOpenSubGroup: 'Message brokers',
    },
    {
      path: '/reference/configuration/',
      expectedOpenGroup: 'Reference',
    },
  ];

  for (const { path: pagePath, expectedOpenGroup, expectedOpenSubGroup } of testPages) {
    it(`verifies sidebar on ${pagePath} shows <= 30 links and only current group is open`, () => {
      assert.ok(existsSync(distDir), 'dist directory must exist');
      const normPath = pagePath.replace(/^\/|\/$/g, '');
      const filePath = path.join(distDir, normPath, 'index.html');
      assert.ok(existsSync(filePath), `HTML file for ${pagePath} must exist at ${filePath}`);

      const html = readFileSync(filePath, 'utf8');
      const { openGroups, closedGroups, visibleLinks } = parseSidebar(html);

      assert.ok(
        visibleLinks.length <= 30,
        `Page ${pagePath} shows ${visibleLinks.length} sidebar links on load, expected at most 30`
      );

      // Verify the current group is open
      assert.ok(
        openGroups.includes(expectedOpenGroup),
        `Group "${expectedOpenGroup}" must be open on ${pagePath}. Open groups: ${JSON.stringify(openGroups)}`
      );

      // Verify other top-level groups are not open
      const topLevelGroups = [
        'Get started',
        'Guides',
        'Security',
        'Rules (SQL)',
        'Connectors',
        'Operate',
        'Reference',
      ];
      for (const group of topLevelGroups) {
        if (group !== expectedOpenGroup) {
          assert.ok(
            !openGroups.includes(group),
            `Group "${group}" must NOT be open on ${pagePath}. Open groups: ${JSON.stringify(openGroups)}`
          );
        }
      }

      // If a sub-group was expected open, verify it
      if (expectedOpenSubGroup) {
        assert.ok(
          openGroups.includes(expectedOpenSubGroup),
          `Sub-group "${expectedOpenSubGroup}" must be open on ${pagePath}`
        );
      }
    });
  }

  it('verifies every redirect source in config resolves to a built page', () => {
    assert.ok(existsSync(distDir), 'dist directory must exist');

    const redirects = parseRedirectsFromConfig();
    const entries = Object.entries(redirects);
    assert.ok(entries.length >= 25, `Expected at least 25 redirects, found ${entries.length}`);

    for (const [source, target] of entries) {
      // Source html must exist in dist
      const normSource = source.replace(/^\/|\/$/g, '');
      const sourceFile = path.join(distDir, normSource, 'index.html');
      assert.ok(
        existsSync(sourceFile),
        `Redirect source HTML must exist at ${sourceFile} for ${source}`
      );

      // Source HTML must contain a redirect to target
      const sourceHtml = readFileSync(sourceFile, 'utf8');
      assert.ok(
        sourceHtml.includes(target) || sourceHtml.includes(target.replace(/\/$/, '')),
        `Redirect file ${sourceFile} must point to target ${target}`
      );

      // Target html must exist in dist
      const normTarget = target.replace(/^\/|\/$/g, '');
      const targetFile = path.join(distDir, normTarget, 'index.html');
      assert.ok(
        existsSync(targetFile),
        `Redirect target HTML must exist at ${targetFile} for ${target}`
      );
    }
  });
});
