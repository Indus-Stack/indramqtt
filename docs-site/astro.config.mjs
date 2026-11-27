import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { fileURLToPath } from 'node:url';
import { readFileSync, existsSync } from 'node:fs';
import remarkMdLinks from './remark-md-links.mjs';
import { remarkMermaid } from './src/plugins/mermaid.mjs';

function mermaidIntegration() {
  return {
    name: 'indramqtt-mermaid',
    hooks: {
      'astro:config:setup': ({ injectScript }) => {
        injectScript('page', 'import "/src/scripts/mermaid-client.js";');
      },
    },
  };
}

const connectorsDataPath = fileURLToPath(new URL('./src/data/connectors.json', import.meta.url));
let connectorsData = [];
if (existsSync(connectorsDataPath)) {
  connectorsData = JSON.parse(readFileSync(connectorsDataPath, 'utf8'));
}

const CATEGORY_ORDER = [
  'Relational databases',
  'Time-series databases',
  'Document stores',
  'Storage and analytics',
  'Message brokers',
  'Cloud messaging',
  'Cloud IoT',
  'Industrial devices',
  'HTTP',
  'Local',
];

const connectorSubGroups = CATEGORY_ORDER.map((category) => {
  const items = connectorsData
    .filter((c) => c.category === category)
    .sort((a, b) => a.title.localeCompare(b.title))
    .map((c) => ({
      label: c.title,
      slug: `connectors/${c.page}`,
    }));
  return {
    label: category,
    collapsed: true,
    items,
  };
}).filter((g) => g.items.length > 0);

// Static site for the user documentation. It reads `docs/` at build
// time (see the `sync-docs` script in `package.json`).
export default defineConfig({
  redirects: {
    '/connectors/aws-iot/': '/connectors/aws_iot/',
    '/connectors/azure-blob/': '/connectors/azure_blob/',
    '/connectors/azure_blob_storage/': '/connectors/azure_blob/',
    '/connectors/azure-event-hubs/': '/connectors/azure_eventhubs/',
    '/connectors/azure_event_hub_producer/': '/connectors/azure_eventhubs/',
    '/connectors/azure-iot/': '/connectors/azure_iot/',
    '/connectors/confluent_producer/': '/connectors/confluent/',
    '/connectors/disk-log/': '/connectors/disk_log/',
    '/connectors/dynamo/': '/connectors/dynamodb/',
    '/connectors/gcp-pubsub/': '/connectors/gcp_pubsub/',
    '/connectors/gcp_pubsub_producer/': '/connectors/gcp_pubsub/',
    '/connectors/gcp_pubsub_consumer/': '/connectors/gcp_pubsub/',
    '/connectors/kafka_producer/': '/connectors/kafka/',
    '/connectors/kafka_consumer/': '/connectors/kafka/',
    '/connectors/mqtt/': '/connectors/mqtt-bridge/',
    '/connectors/sqlserver/': '/connectors/mssql/',
    '/connectors/oci-streaming/': '/connectors/oci_streaming/',
    '/connectors/opc-ua/': '/connectors/opc_ua/',
    '/connectors/opents/': '/connectors/opentsdb/',
    '/connectors/postgres/': '/connectors/pgsql/',
    '/connectors/s3tables/': '/connectors/s3_tables/',
    '/connectors/snowflake_streaming/': '/connectors/snowflake/',
    '/connectors/sparkplug-b/': '/connectors/sparkplug_b/',
    '/connectors/timescale/': '/connectors/timescaledb/',
    '/connectors/aws_timestream/': '/connectors/timestream/',
  },
  markdown: {
    remarkPlugins: [
      remarkMermaid,
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
    mermaidIntegration(),
    starlight({
      title: 'IndraMQTT',
      description: 'The complete MQTT platform in one small binary.',
      logo: {
        src: './src/assets/mark.svg',
        replacesTitle: false,
      },
      social: {
        github: 'https://github.com/ankur-paan/indramqtt',
      },
      editLink: {
        baseUrl: 'https://github.com/ankur-paan/indramqtt/edit/main/docs/',
      },
      customCss: ['./src/styles/theme.css', './src/styles/panels.css'],
      components: {
        Head: './src/components/overrides/Head.astro',
        PageFrame: './src/components/overrides/PageFrame.astro',
        PageSidebar: './src/components/overrides/PageSidebar.astro',
        MarkdownContent: './src/components/overrides/MarkdownContent.astro',
      },
      sidebar: [
        {
          label: 'Get started',
          collapsed: true,
          items: [
            { label: 'Overview', slug: 'getting-started' },
            { label: 'Install', slug: 'getting-started/install' },
            { label: 'First start', slug: 'getting-started/first-start' },
            { label: 'First publish', slug: 'getting-started/first-publish' },
            { label: 'First user', slug: 'getting-started/first-user' },
            { label: 'Console', slug: 'getting-started/console' },
          ],
        },
        {
          label: 'Guides',
          collapsed: true,
          items: [
            { label: 'Overview', slug: 'concepts' },
            { label: 'Listeners', slug: 'concepts/listeners' },
            { label: 'CoAP gateway', slug: 'concepts/coap-gateway' },
            { label: 'Multi-tenancy', slug: 'multi-tenancy' },
            { label: 'Tenant quotas and administration', slug: 'multi-tenancy/quotas' },
            { label: 'Migration', slug: 'migration' },
            { label: 'MCP', slug: 'mcp' },
          ],
        },
        {
          label: 'Security',
          collapsed: true,
          items: [
            { label: 'Overview', slug: 'security' },
            { label: 'TLS and client certificates', slug: 'security/tls' },
            { label: 'Pre-shared keys', slug: 'security/psk' },
            { label: 'Authentication', slug: 'security/authentication' },
            { label: 'Built-in database example', slug: 'security/authentication-builtin' },
            { label: 'JWT example', slug: 'security/authentication-jwt' },
            { label: 'Authorization', slug: 'security/authorization' },
            { label: 'Management users, MFA and API keys', slug: 'security/management-access' },
            { label: 'Audit log', slug: 'security/audit-log' },
            { label: 'Re-authentication', slug: 'security/re-authentication' },
          ],
        },
        {
          label: 'Rules (SQL)',
          collapsed: true,
          items: [
            { label: 'Overview', slug: 'rules' },
            { label: 'Rule SQL', slug: 'rules/sql' },
            { label: 'Rule clauses and operators', slug: 'rules/clauses-operators' },
            { label: 'Rule events', slug: 'rules/events' },
            { label: 'Rule functions', slug: 'rules/functions' },
            { label: 'Rule functions: message context', slug: 'rules/functions-context' },
            { label: 'Rule functions: math and bitwise', slug: 'rules/functions-math-bitwise' },
            { label: 'Rule functions: bit and byte', slug: 'rules/functions-bits-bytes' },
            { label: 'Rule functions: type conversion', slug: 'rules/functions-type-conversion' },
            { label: 'Rule functions: type checks', slug: 'rules/functions-type-checks' },
            { label: 'Rule functions: arrays', slug: 'rules/functions-arrays' },
            { label: 'Rule functions: hashing', slug: 'rules/functions-hashing' },
            { label: 'Rule functions: compression', slug: 'rules/functions-compression' },
            { label: 'Rule functions: rule state', slug: 'rules/functions-state' },
            { label: 'Rule actions', slug: 'rules/actions' },
          ],
        },
        {
          label: 'Connectors',
          collapsed: true,
          items: [
            { label: 'Overview', slug: 'connectors' },
            ...connectorSubGroups,
          ],
        },
        {
          label: 'Operate',
          collapsed: true,
          items: [
            { label: 'Overview', slug: 'operations' },
            { label: 'Deployment', slug: 'operations/deployment' },
            { label: 'Configuration', slug: 'operations/configuration' },
            { label: 'Sizing', slug: 'operations/sizing' },
            { label: 'Monitoring', slug: 'operations/monitoring' },
            { label: 'Logs', slug: 'operations/logging' },
            { label: 'Backup and restore', slug: 'operations/backup' },
            { label: 'Upgrade', slug: 'operations/upgrade' },
            { label: 'Troubleshooting', slug: 'operations/troubleshooting' },
          ],
        },
        {
          label: 'Reference',
          collapsed: true,
          items: [
            { label: 'Overview', slug: 'reference' },
            {
              label: 'API reference',
              collapsed: true,
              items: [
                { label: 'Overview', slug: 'reference/api' },
                { label: 'Authentication & ACL', slug: 'reference/api/authentication-acl' },
                { label: 'Clients & Sessions', slug: 'reference/api/clients-sessions' },
                { label: 'Cluster & Nodes', slug: 'reference/api/cluster-nodes' },
                { label: 'Configuration', slug: 'reference/api/configuration' },
                { label: 'Data Connectors', slug: 'reference/api/data-connectors' },
                { label: 'Licence', slug: 'reference/api/licence' },
                { label: 'Observability', slug: 'reference/api/observability' },
                { label: 'Rule Engine', slug: 'reference/api/rule-engine' },
              ],
            },
            {
              label: 'Connector actions',
              collapsed: true,
              items: [
                { label: 'Connector actions: AlloyDB to DynamoDB', slug: 'rules/actions-a' },
                { label: 'Connector actions: Elasticsearch to OpenTSDB', slug: 'rules/actions-b' },
                { label: 'Connector actions: Oracle to Webhook', slug: 'rules/actions-c' },
              ],
            },
            { label: 'Command line reference', slug: 'reference/cli' },
            { label: 'Configuration reference', slug: 'reference/configuration' },
            { label: 'Connector, action and source kinds', slug: 'reference/connector-kinds' },
            { label: 'MCP tools catalogue', slug: 'reference/mcp-tools' },
            { label: 'Metrics reference', slug: 'reference/metrics' },
            { label: 'Removed connectors', slug: 'reference/removed-connectors' },
            { label: 'Settings reference', slug: 'reference/settings' },
            { label: 'Sources', slug: 'reference/sources' },
            { label: 'Stored setting audit', slug: 'reference/unapplied-settings' },
            { label: 'Verification failures', slug: 'reference/verification-failures' },
          ],
        },
        { label: 'Benchmarks', slug: 'benchmarks' },
      ],
    }),
  ],
});
