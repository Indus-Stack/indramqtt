// Test that pages with fewer than 3 headings have no page panel,
// and pages with 3 or more headings have it (DS-02, R-NOFAKE).
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { describe, it } from 'node:test';

describe('collapsible sidebars and page panel tests', () => {
  it('fails when dist directory is absent', () => {
    const distDir = path.resolve('dist');
    assert.ok(existsSync(distDir), 'dist directory must exist. Run npm run build first.');
  });

  it('verifies /reference/connector-kinds/ (fewer than 3 headings) has no page panel and no drop-down', () => {
    const distDir = path.resolve('dist');
    const filePath = path.join(distDir, 'reference', 'connector-kinds', 'index.html');
    assert.ok(existsSync(filePath), 'reference/connector-kinds/index.html must exist in dist');

    const html = readFileSync(filePath, 'utf8');

    // Page must not have table of contents elements or drop-down
    assert.ok(
      !html.includes('<starlight-toc'),
      'Page with fewer than 3 headings must not contain <starlight-toc>'
    );
    assert.ok(
      !html.includes('mobile-starlight-toc'),
      'Page with fewer than 3 headings must not contain mobile-starlight-toc'
    );
    assert.ok(
      !html.includes('id="indra-toggle-toc"'),
      'Page with fewer than 3 headings must not render TOC toggle button'
    );
    assert.ok(
      !html.includes('id="starlight__on-this-page"'),
      'Page with fewer than 3 headings must not render "On this page" heading'
    );

    // But it must have the sidebar toggle button
    assert.ok(html.includes('id="indra-toggle-sidebar"'), 'Page must have sidebar toggle button');
    assert.ok(
      html.includes('aria-controls="starlight__sidebar"'),
      'Sidebar toggle button must have aria-controls="starlight__sidebar"'
    );
  });

  it('verifies /reference/configuration/ (3 or more headings) has the page panel and toggle button', () => {
    const distDir = path.resolve('dist');
    const filePath = path.join(distDir, 'reference', 'configuration', 'index.html');
    assert.ok(existsSync(filePath), 'reference/configuration/index.html must exist in dist');

    const html = readFileSync(filePath, 'utf8');

    // Page must have table of contents elements
    assert.ok(
      html.includes('<starlight-toc'),
      'Page with 3 or more headings must contain <starlight-toc>'
    );
    assert.ok(
      html.includes('id="indra-toggle-toc"'),
      'Page with 3 or more headings must render TOC toggle button'
    );
    assert.ok(
      html.includes('aria-controls="starlight__toc"'),
      'TOC toggle button must have aria-controls="starlight__toc"'
    );
    assert.ok(
      html.includes('id="starlight__on-this-page"'),
      'Page with 3 or more headings must render "On this page" heading'
    );

    // And sidebar toggle button
    assert.ok(html.includes('id="indra-toggle-sidebar"'), 'Page must have sidebar toggle button');
    assert.ok(
      html.includes('aria-controls="starlight__sidebar"'),
      'Sidebar toggle button must have aria-controls="starlight__sidebar"'
    );

    // Verify configuration page has no h3 entry in its page panel (DS-05, item 3)
    const tocMatch = html.match(/<starlight-toc\b[^>]*>([\s\S]*?)<\/starlight-toc>/);
    assert.ok(tocMatch, 'configuration page must have <starlight-toc>');
    const tocHtml = tocMatch[1];

    // Read markdown to check h2 and h3 headings
    const repoRoot = path.resolve('..');
    const configMd = readFileSync(
      path.join(repoRoot, 'docs', 'reference', 'configuration.md'),
      'utf8'
    );
    const mdLines = configMd.split('\n');
    const h2Slugs = [];
    const h3Slugs = [];
    for (const line of mdLines) {
      if (/^##\s+/.test(line)) {
        const text = line.replace(/^##\s+/, '').trim();
        const slug = text.toLowerCase().replace(/[^\w\s-]/g, '').replace(/\s+/g, '-');
        h2Slugs.push(slug);
      } else if (/^###\s+/.test(line)) {
        const text = line.replace(/^###\s+/, '').trim();
        const slug = text.toLowerCase().replace(/[^\w\s-]/g, '').replace(/\s+/g, '-');
        h3Slugs.push(slug);
      }
    }

    assert.ok(h2Slugs.length > 0, 'configuration must have h2 headings');
    assert.ok(h3Slugs.length > 0, 'configuration must have h3 headings');

    // Page panel must list h2 headings but not h3 headings
    assert.ok(
      h2Slugs.some((slug) => tocHtml.includes(`href="#${slug}"`)),
      'configuration page panel must list h2 headings'
    );
    for (const slug of h3Slugs) {
      assert.ok(
        !tocHtml.includes(`href="#${slug}"`),
        `configuration page panel must not include h3 heading #${slug}`
      );
    }
  });

  it('verifies /getting-started/install/ keeps all its panel entries (DS-05, item 3)', () => {
    const distDir = path.resolve('dist');
    const filePath = path.join(distDir, 'getting-started', 'install', 'index.html');
    assert.ok(existsSync(filePath), 'getting-started/install/index.html must exist in dist');

    const html = readFileSync(filePath, 'utf8');
    const tocMatch = html.match(/<starlight-toc\b[^>]*>([\s\S]*?)<\/starlight-toc>/);
    assert.ok(tocMatch, 'install page must have <starlight-toc>');
    const tocHtml = tocMatch[1];

    const repoRoot = path.resolve('..');
    const installMd = readFileSync(
      path.join(repoRoot, 'docs', 'getting-started', 'install.md'),
      'utf8'
    );
    const mdLines = installMd.split('\n');
    const headings = [];
    for (const line of mdLines) {
      if (/^##\s+/.test(line)) {
        const text = line.replace(/^##\s+/, '').trim();
        const slug = text.toLowerCase().replace(/[^\w\s-]/g, '').replace(/\s+/g, '-');
        headings.push(slug);
      }
    }

    assert.ok(headings.length > 0, 'install must have headings');
    for (const slug of headings) {
      assert.ok(
        tocHtml.includes(`href="#${slug}"`),
        `install page panel must retain heading #${slug}`
      );
    }
  });
});
