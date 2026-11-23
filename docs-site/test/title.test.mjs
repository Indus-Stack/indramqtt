// Test that every page in dist has exactly one h1 heading (DS-01, R-NOFAKE).
import assert from 'node:assert/strict';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { describe, it } from 'node:test';

function collectHtmlFiles(dir) {
  const results = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...collectHtmlFiles(fullPath));
    } else if (entry.isFile() && entry.name.endsWith('.html')) {
      results.push(fullPath);
    }
  }
  return results;
}

describe('single page title test', () => {
  it('fails when dist directory is absent', () => {
    const distDir = path.resolve('dist');
    assert.ok(existsSync(distDir), 'dist directory must exist. Run npm run build first.');
  });

  it('verifies every page in dist has exactly one h1 heading', () => {
    const distDir = path.resolve('dist');
    assert.ok(existsSync(distDir), 'dist directory must exist');

    const htmlFiles = collectHtmlFiles(distDir);
    assert.ok(htmlFiles.length > 0, 'dist must contain HTML files');

    const failures = [];
    for (const file of htmlFiles) {
      const content = readFileSync(file, 'utf8');
      if (content.includes('http-equiv="refresh"')) continue;
      const h1Matches = content.match(/<h1\b[^>]*>([\s\S]*?)<\/h1>/gi) || [];
      if (h1Matches.length !== 1) {
        failures.push({
          file: path.relative(distDir, file),
          count: h1Matches.length,
          headings: h1Matches,
        });
      }
    }

    assert.equal(
      failures.length,
      0,
      `Found ${failures.length} pages with invalid h1 count:\n${JSON.stringify(failures, null, 2)}`
    );
  });
});
