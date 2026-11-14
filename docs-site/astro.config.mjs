import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { fileURLToPath } from 'node:url';
import remarkMdLinks from './remark-md-links.mjs';

// Static site for the user documentation. It reads `docs/` at build
// time (see the `sync-docs` script in `package.json`).
export default defineConfig({
  // The docs mark expected command output with `output` fences.
  // This name joins the highlighter so the build prints no warning.
  markdown: {
    // Turns the file links of the docs (`./sql.md`) into page URLs (`/rules/sql/`).
    remarkPlugins: [
      [remarkMdLinks, { docsRoot: fileURLToPath(new URL('./src/content/docs', import.meta.url)) }],
    ],
    shikiConfig: {
      langs: [
        {
          name: 'output',
          scopeName: 'text.output',
          patterns: [{ match: '.*', name: 'text.output' }],
        },
      ],
    },
  },
  integrations: [
    starlight({
      title: 'IndraMQTT',
      description: 'The complete MQTT platform in one small binary.',
      components: {
        Head: './src/components/overrides/Head.astro',
      },
      sidebar: [
        {
          label: 'Start here',
          items: [{ label: 'IndraMQTT', slug: 'index' }],
        },
        { label: 'Getting started', autogenerate: { directory: 'getting-started' } },
        { label: 'Concepts', autogenerate: { directory: 'concepts' } },
        { label: 'Security', autogenerate: { directory: 'security' } },
        { label: 'Rules', autogenerate: { directory: 'rules' } },
        { label: 'Connectors', autogenerate: { directory: 'connectors' } },
        { label: 'Multi-tenancy', autogenerate: { directory: 'multi-tenancy' } },
        { label: 'Operations', autogenerate: { directory: 'operations' } },
        { label: 'Reference', autogenerate: { directory: 'reference' } },
        { label: 'Benchmarks', autogenerate: { directory: 'benchmarks' } },
        {
          label: 'More',
          items: [
            { label: 'Migration', slug: 'migration' },
            { label: 'MCP', slug: 'mcp' },
          ],
        },
      ],
    }),
  ],
});
