/**
 * Databricks Primary Icon Registry
 *
 * Official Databricks brand icons curated for the dbxWearables-ZeroBus gateway app.
 * SVGs are bundled locally in /images/icons/ (downloaded from Brandfolder CDN).
 *
 * Source catalog: fixtures/icons/databricks-primary-icons-collection.csv (826 icons)
 * CDN base: https://cdn.bfldr.com/9AYANS2F/at/{id}/{filename}.svg
 *
 * Usage:
 *   import { ICONS, icon } from '../icons';
 *   <img src={icon('streaming')} alt="Streaming" className="w-8 h-8" />
 */

export const ICONS = {
  // ── Health & Wearables ──────────────────────────────────────
  'clinical-data':        '/images/icons/primary-icon-white-clinical-data.svg',
  'healthcare':           '/images/icons/primary-icon-navy-healthcare.svg',
  'healthcare-white':     '/images/icons/primary-icon-white-healthcare.svg',
  'hospital':             '/images/icons/primary-icon-orange-hospital.svg',
  'human':                '/images/icons/primary-icon-white-human.svg',

  // ── Streaming & Ingest ──────────────────────────────────────
  'streaming':            '/images/icons/primary-icon-orange-streaming.svg',
  'data-flow':            '/images/icons/primary-icon-orange-data-flow.svg',
  'real-time':            '/images/icons/primary-icon-white-real-time-pricing.svg',
  'spark-streaming':      '/images/icons/primary-icon-white-spark-streaming-job.svg',
  'automation':           '/images/icons/primary-icon-white-automation.svg',

  // ── Architecture ────────────────────────────────────────────
  'lakehouse':            '/images/icons/primary-icon-navy-lakehouse.svg',
  'delta-table':          '/images/icons/primary-icon-navy-delta-table.svg',
  'unity-catalog':        '/images/icons/primary-icon-white-unity-catalog.svg',
  'lakeflow':             '/images/icons/primary-icon-lakeflow-white.svg',
  'lakeflow-pipelines':   '/images/icons/primary-icon-lakeflow-pipelines-white.svg',
  'lakeflow-connect':     '/images/icons/primary-icon-lakeflow-connect-white.svg',
  'data-warehouse':       '/images/icons/primary-icon-orange-data-warehouse.svg',
  'delta-live-tables':    '/images/icons/primary-icon-white-delta-live-tables.svg',
  'asset-bundle':         '/images/icons/primary-icon-white-asset-bundle.svg',

  // ── Medallion ───────────────────────────────────────────────
  'unstructured-bronze':  '/images/icons/primary-icon-white-unstructured-bronze.svg',
  'semi-structured-silver': '/images/icons/primary-icon-orange-semi-structured-silver.svg',
  'structured-gold':      '/images/icons/primary-icon-white-structured-gold.svg',

  // ── Security & Auth ─────────────────────────────────────────
  'authentication':       '/images/icons/primary-icon-orange-authentication-service.svg',
  'data-security':        '/images/icons/primary-icon-white-data-security.svg',
  'cloud-security':       '/images/icons/primary-icon-white-cloud-security.svg',
  'privacy':              '/images/icons/primary-icon-orange-privacy.svg',
  'encryption':           '/images/icons/primary-icon-white-encryption.svg',
  'gdpr':                 '/images/icons/primary-icon-white-gdpr.svg',
  'compliance':           '/images/icons/primary-icon-white-compliance.svg',

  // ── Devices & IoT ───────────────────────────────────────────
  'iot':                  '/images/icons/primary-icon-white-iot.svg',
  'smartphone':           '/images/icons/primary-icon-white-phone.svg',

  // ── API & Dev ───────────────────────────────────────────────
  'sql':                  '/images/icons/primary-icon-white-sql.svg',
  'notebook':             '/images/icons/primary-icon-orange-data-science-notebook.svg',
  'notebook-white':       '/images/icons/primary-icon-white-notebook.svg',
  'apps-services':        '/images/icons/primary-icon-navy-apps-services.svg',
  'webhook':              '/images/icons/primary-icon-white-webhook.svg',
  'endpoint':             '/images/icons/primary-icon-white-endpoint.svg',
  'data-source-apis':     '/images/icons/primary-icon-white-data-source-apis.svg',
  'dashboards':           '/images/icons/primary-icon-white-dashboards.svg',
  'analytics':            '/images/icons/primary-icon-white-magnify-analytics.svg',
  'partner-connect':      '/images/icons/primary-icon-white-partner-connect.svg',
} as const;

/** All valid icon keys */
export type IconKey = keyof typeof ICONS;

/** CDN fallback URLs (stable, no expiry) — use if local bundle is unavailable */
export const ICON_CDN: Record<IconKey, string> = {
  // ── Health & Wearables
  'clinical-data':        'https://cdn.bfldr.com/9AYANS2F/at/n5bw4fbp7w5pxb2vkmpqc4gs/primary-icon-white-clinical-data.svg',
  'healthcare':           'https://cdn.bfldr.com/9AYANS2F/at/q5t5kwwqrh44c92hxn6t34rs/primary-icon-navy-healthcare.svg',
  'healthcare-white':     'https://cdn.bfldr.com/9AYANS2F/at/csnpwbv32z2zxgv5t6n6p6s/primary-icon-white-healthcare.svg',
  'hospital':             'https://cdn.bfldr.com/9AYANS2F/at/6nh3nhfb5vg63f9g6v8fpfrx/primary-icon-orange-hospital.svg',
  'human':                'https://cdn.bfldr.com/9AYANS2F/at/c8x567cqh74f2c65jg7gjfkm/primary-icon-white-human.svg',
  // ── Streaming & Ingest
  'streaming':            'https://cdn.bfldr.com/9AYANS2F/at/56m9rz8jwtqkvt8wmvvb5pj/primary-icon-orange-streaming.svg',
  'data-flow':            'https://cdn.bfldr.com/9AYANS2F/at/6v49pjgkm579rs2rm69hbtgk/primary-icon-orange-data-flow.svg',
  'real-time':            'https://cdn.bfldr.com/9AYANS2F/at/jt34jqns7j39w4ckcqmn9fvv/primary-icon-white-real-time-pricing.svg',
  'spark-streaming':      'https://cdn.bfldr.com/9AYANS2F/at/rp3mvtnm6tgf6w9mps8jbxj8/primary-icon-white-spark-streaming-job.svg',
  'automation':           'https://cdn.bfldr.com/9AYANS2F/at/cfn5qqqx4855hjjrjx3hjmq/primary-icon-white-automation.svg',
  // ── Architecture
  'lakehouse':            'https://cdn.bfldr.com/9AYANS2F/at/g3png8q6vh7pm8wmjkgwrr/primary-icon-navy-lakehouse.svg',
  'delta-table':          'https://cdn.bfldr.com/9AYANS2F/at/sspmscjwjgr3kzbbw555mcc/primary-icon-navy-delta-table.svg',
  'unity-catalog':        'https://cdn.bfldr.com/9AYANS2F/at/2k582scp55m6sgcw47358j/primary-icon-white-unity-catalog.svg',
  'lakeflow':             'https://cdn.bfldr.com/9AYANS2F/at/hc5xsvwks6rvjt78nhmrrpfs/primary-icon-lakeflow-white.svg',
  'lakeflow-pipelines':   'https://cdn.bfldr.com/9AYANS2F/at/rs5kcb74q92f6mqgr6v8rjm/primary-icon-lakeflow-pipelines-white.svg',
  'lakeflow-connect':     'https://cdn.bfldr.com/9AYANS2F/at/743kgxck598qtqrmxpskhvns/primary-icon-lakeflow-connect-white.svg',
  'data-warehouse':       'https://cdn.bfldr.com/9AYANS2F/at/g498k3bwhxj2bm7txhj4pvt/primary-icon-orange-data-warehouse.svg',
  'delta-live-tables':    'https://cdn.bfldr.com/9AYANS2F/at/sm89rr5nbj3wg5fwq99q5cnk/primary-icon-white-delta-live-tables.svg',
  'asset-bundle':         'https://cdn.bfldr.com/9AYANS2F/at/hxgggp5gct6mwz38jtrvsh6q/primary-icon-white-asset-bundle.svg',
  // ── Medallion
  'unstructured-bronze':  'https://cdn.bfldr.com/9AYANS2F/at/8xgrcr2jkhjtcx3sm8q64wjj/primary-icon-white-unstructured-bronze.svg',
  'semi-structured-silver': 'https://cdn.bfldr.com/9AYANS2F/at/9j4hvhkggsbpgbrtgqswpq83/primary-icon-orange-semi-structured-silver.svg',
  'structured-gold':      'https://cdn.bfldr.com/9AYANS2F/at/m6qk2bhns8nx2vhhpj8kvzn4/primary-icon-white-structured-gold.svg',
  // ── Security & Auth
  'authentication':       'https://cdn.bfldr.com/9AYANS2F/at/6pg9bx63tk8b446v4xnzrt/primary-icon-orange-authentication-service.svg',
  'data-security':        'https://cdn.bfldr.com/9AYANS2F/at/c8hqrm9vvk877q8stj6z2gh5/primary-icon-white-data-security.svg',
  'cloud-security':       'https://cdn.bfldr.com/9AYANS2F/at/mv6xmvr4h6776b9m9q7gzb9/primary-icon-white-cloud-security.svg',
  'privacy':              'https://cdn.bfldr.com/9AYANS2F/at/7j24j7tsfz5f865mk7wntcwr/primary-icon-orange-privacy.svg',
  'encryption':           'https://cdn.bfldr.com/9AYANS2F/at/58g6n374tgvq9v4pp345m6h/primary-icon-white-encryption.svg',
  'gdpr':                 'https://cdn.bfldr.com/9AYANS2F/at/5qbhxh56qr7tsmqgs54q9g/primary-icon-white-gdpr.svg',
  'compliance':           'https://cdn.bfldr.com/9AYANS2F/at/c5jjfsk8pchqcr4b9mqq9mx/primary-icon-white-compliance.svg',
  // ── Devices & IoT
  'iot':                  'https://cdn.bfldr.com/9AYANS2F/at/5w9jgjbg788mr5sz4sqk7zr5/primary-icon-white-iot.svg',
  'smartphone':           'https://cdn.bfldr.com/9AYANS2F/at/j9fk34s6sp6bknjrx7ppkk7k/primary-icon-white-phone.svg',
  // ── API & Dev
  'sql':                  'https://cdn.bfldr.com/9AYANS2F/at/2z99shgzcjmb5wf88s2h4mb4/primary-icon-white-sql.svg',
  'notebook':             'https://cdn.bfldr.com/9AYANS2F/at/kjfxgrt5pqk39fv8j65mkpmx/primary-icon-orange-data-science-notebook.svg',
  'notebook-white':       'https://cdn.bfldr.com/9AYANS2F/at/4ffrpmwjqm2q8bt33m2mtn3g/primary-icon-white-notebook.svg',
  'apps-services':        'https://cdn.bfldr.com/9AYANS2F/at/vhhwc75z44wf85nph77bhwcq/primary-icon-navy-apps-services.svg',
  'webhook':              'https://cdn.bfldr.com/9AYANS2F/at/m8ktmhxxcmgjh39stm54wjq6/primary-icon-white-webhook.svg',
  'endpoint':             'https://cdn.bfldr.com/9AYANS2F/at/4tmc37p5v6crzcjx4vp7mw/primary-icon-white-endpoint.svg',
  'data-source-apis':     'https://cdn.bfldr.com/9AYANS2F/at/n45jqc89mcxb8t5vgg59nqxk/primary-icon-white-data-source-apis.svg',
  'dashboards':           'https://cdn.bfldr.com/9AYANS2F/at/w6qtkn56br5qn87h4fsqvm84/primary-icon-white-dashboards.svg',
  'analytics':            'https://cdn.bfldr.com/9AYANS2F/at/h53mjcxspfgkbkq5kj4skhcb/primary-icon-white-magnify-analytics.svg',
  'partner-connect':      'https://cdn.bfldr.com/9AYANS2F/at/5mg5pgk5pbq7544x6z9k2j/primary-icon-white-partner-connect.svg',
};

/**
 * Get the local path for a Databricks primary icon.
 * Use in <img src={icon('streaming')} /> — Vite serves from public/.
 */
export function icon(key: IconKey): string {
  return ICONS[key];
}

/**
 * Get the CDN URL for a Databricks primary icon (fallback).
 * Stable URLs on cdn.bfldr.com — no auth or expiry required.
 */
export function iconCdn(key: IconKey): string {
  return ICON_CDN[key];
}
