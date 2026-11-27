// Test connector catalogue data generation and structure (DS-03, R-NOFAKE).
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { describe, it } from 'node:test';

describe('connector catalogue tests', () => {
  it('fails when connectors.json is absent', () => {
    const jsonPath = path.resolve('src/data/connectors.json');
    assert.ok(
      existsSync(jsonPath),
      'connectors.json must exist. Run npm run sync-docs or npm run build first.'
    );
  });

  it('verifies connectors.json has exactly 50 entries and valid schema', () => {
    const jsonPath = path.resolve('src/data/connectors.json');
    assert.ok(existsSync(jsonPath), 'connectors.json must exist');

    const raw = readFileSync(jsonPath, 'utf8');
    const data = JSON.parse(raw);

    assert.equal(data.length, 50, 'catalogue must have exactly 50 entries');

    const repoRoot = path.resolve('..');
    const connectorsDir = path.join(repoRoot, 'docs', 'connectors');

    const validStates = new Set(['Ready', 'Pending', 'Not in this build']);

    for (const item of data) {
      assert.ok(item.kind, `kind must be non-empty string in ${JSON.stringify(item)}`);
      assert.ok(Array.isArray(item.aliases), `aliases must be an array in ${item.kind}`);
      assert.ok(item.category, `category must be non-empty string in ${item.kind}`);
      assert.ok(validStates.has(item.state), `state ${item.state} invalid in ${item.kind}`);
      assert.notEqual(item.kind, 'test', 'test-only connector must not be in catalogue');
      assert.notEqual(item.state, 'test-only', 'test-only state must not be in catalogue');
      assert.ok(item.title, `title must be non-empty in ${item.kind}`);
      assert.ok(item.description, `description must be non-empty in ${item.kind}`);
      assert.ok(item.page, `page must be non-empty in ${item.kind}`);

      const pageFile = path.join(connectorsDir, `${item.page}.md`);
      assert.ok(existsSync(pageFile), `Page file ${pageFile} must exist for kind ${item.kind}`);
    }

    // Verify sorted by state (Ready, Pending, Not in this build) then title (DS-05, item 4)
    const STATE_ORDER = {
      'Ready': 0,
      'Pending': 1,
      'Not in this build': 2,
    };

    for (let i = 0; i < data.length - 1; i++) {
      const orderA = STATE_ORDER[data[i].state] ?? 99;
      const orderB = STATE_ORDER[data[i + 1].state] ?? 99;
      assert.ok(
        orderA <= orderB,
        `connectors must be ordered by state (Ready, Pending, Not in this build): "${data[i].title}" (${data[i].state}) vs "${data[i + 1].title}" (${data[i + 1].state})`
      );
      if (orderA === orderB) {
        assert.ok(
          data[i].title.localeCompare(data[i + 1].title) <= 0,
          `connectors in state "${data[i].state}" must be sorted by title: "${data[i].title}" vs "${data[i + 1].title}"`
        );
      }
    }
  });
});
