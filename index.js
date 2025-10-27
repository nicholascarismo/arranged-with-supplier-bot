import 'dotenv/config';
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import boltPkg from '@slack/bolt';

/* =========================
   Slack (Socket Mode)
========================= */
const { App } = boltPkg;

/* =========================
   Env & Config
========================= */
const {
  // Slack
  SLACK_BOT_TOKEN,
  SLACK_APP_TOKEN,
  SLACK_SIGNING_SECRET,

  // Where to post notifications by default
  WATCH_CHANNEL_ID,

  // Shopify Admin
  SHOPIFY_DOMAIN,                 // e.g. mystore.myshopify.com
  SHOPIFY_ADMIN_TOKEN,            // Admin API access token (private app or custom app)
  SHOPIFY_API_VERSION = '2025-01',

  // Microsoft Entra App (client credentials for Microsoft Graph)
  MS_TENANT_ID,
  MS_CLIENT_ID,
  MS_CLIENT_SECRET,

  // Polling interval (ms)
  SCAN_INTERVAL_MS = '60000'      // default: 60s
} = process.env;

// Hard validation (fail fast)
function need(keys) {
  const missing = keys.filter(k => !process.env[k] || String(process.env[k]).trim() === '');
  if (missing.length) {
    console.error('Missing required env:', missing.join(', '));
    process.exit(1);
  }
}
need(['SLACK_BOT_TOKEN', 'SLACK_APP_TOKEN', 'SHOPIFY_DOMAIN', 'SHOPIFY_ADMIN_TOKEN', 'MS_TENANT_ID', 'MS_CLIENT_ID', 'MS_CLIENT_SECRET']);

/* =========================
   Persistent Store (./data)
========================= */
const DATA_DIR = path.resolve('./data');
const SEEN_DIR = path.join(DATA_DIR, 'seen');
await fsp.mkdir(DATA_DIR, { recursive: true });
await fsp.mkdir(SEEN_DIR, { recursive: true });

async function readJsonSafe(file, fallback) {
  try {
    const txt = await fsp.readFile(file, 'utf8');
    return JSON.parse(txt);
  } catch {
    return fallback;
  }
}
async function writeJsonAtomic(file, data) {
  const tmp = `${file}.tmp-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  await fsp.writeFile(tmp, JSON.stringify(data, null, 2), 'utf8');
  await fsp.rename(tmp, file);
}

/* =========================
   Supplier Docs (config)
========================= */
// Each entry defines: a unique key, human name, sharing URL for the Excel file,
// worksheet name, columns to monitor, and the supplier label to apply in Shopify.
const SUPPLIER_DOCS = [
  {
    key: 'ohc',
    name: 'Carismo OHC Order Tracking',
    shareUrl: "https://onedrive.live.com/edit.aspx?resid=F03B2DE5BE400EF9!12376&ithint=file%2Cxlsx&authkey=!AEpFUwfpUgvwGGM&activeCell=%27Customer%20Orders%27!A1",
    sheet: 'Customer Orders',
    colOrder: 'B',
    colDate: 'A',
    supplier: 'OHC'
  },
  {
    key: 'bospeed',
    name: 'Carismo Bospeed Order Tracking',
    shareUrl: "https://onedrive.live.com/edit.aspx?resid=F03B2DE5BE400EF9!12635&cid=f03b2de5be400ef9&CT=1720193149957&OR=ItemsView",
    sheet: 'Customer Orders',
    colOrder: 'B',
    colDate: 'A',
    supplier: 'Bospeed'
  },
  {
    key: 'tdd',
    name: 'Carismo TDD Order Tracking',
    shareUrl: "https://onedrive.live.com/personal/f03b2de5be400ef9/_layouts/15/doc2.aspx?resid=F03B2DE5BE400EF9!sd9a8fda925724253b1a4d60fa15fcd7c&cid=f03b2de5be400ef9&migratedtospo=true&app=Excel",
    sheet: 'Customer Orders',
    colOrder: 'B',
    colDate: 'A',
    supplier: 'TDD'
  }
];

/* =========================
   Microsoft Graph (Excel)
========================= */
async function getGraphToken() {
  const url = `https://login.microsoftonline.com/${encodeURIComponent(MS_TENANT_ID)}/oauth2/v2.0/token`;
  const body = new URLSearchParams({
    client_id: MS_CLIENT_ID,
    client_secret: MS_CLIENT_SECRET,
    grant_type: 'client_credentials',
    scope: 'https://graph.microsoft.com/.default'
  });
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body
  });
  if (!res.ok) {
    const t = await res.text().catch(() => '');
    throw new Error(`Graph token failed: ${res.status} ${res.statusText} - ${t}`);
  }
  const json = await res.json();
  return json.access_token;
}

// Convert a sharing URL to a driveItem id via /shares/{encoded}/driveItem
function encodeSharingUrl(u) {
  // base64url of "u!<url>"
  const raw = 'u!' + u;
  const b64 = Buffer.from(raw, 'utf8').toString('base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
  return b64;
}

async function getDriveItemIdFromShare(shareUrl, token) {
  const encoded = encodeSharingUrl(shareUrl);
  const res = await fetch(`https://graph.microsoft.com/v1.0/shares/${encoded}/driveItem`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  if (!res.ok) {
    const t = await res.text().catch(() => '');
    throw new Error(`Graph share lookup failed: ${res.status} ${res.statusText} - ${t}`);
  }
  const json = await res.json();
  return json?.id;
}

// Read entire used range values for a worksheet (valuesOnly)
async function readWorksheetValues(driveItemId, sheetName, token) {
  const url = `https://graph.microsoft.com/v1.0/me/drive/items/${encodeURIComponent(driveItemId)}/workbook/worksheets('${encodeURIComponent(sheetName)}')/usedRange(valuesOnly=true)?$select=values`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` }
  });
  if (!res.ok) {
    const t = await res.text().catch(() => '');
    throw new Error(`Graph usedRange failed: ${res.status} ${res.statusText} - ${t}`);
  }
  const json = await res.json();
  return json?.values || [];
}

// Helper to extract column index from letter (A=0)
function colLetterToIndex(letter) {
  const up = letter.trim().toUpperCase();
  return up.charCodeAt(0) - 'A'.charCodeAt(0);
}

/* =========================
   Shopify Helpers
========================= */
const SHOPIFY_BASE = `https://${SHOPIFY_DOMAIN}/admin/api/${SHOPIFY_API_VERSION}`;

let __gate = Promise.resolve();
const __GAP_MS = 400;
async function __withThrottle(fn) {
  const prev = __gate;
  let release;
  __gate = new Promise(res => { release = res; });
  await prev;
  try {
    return await fn();
  } finally {
    setTimeout(release, __GAP_MS);
  }
}

async function shopifyFetch(pathname, { method = 'GET', headers = {}, body } = {}, attempt = 1) {
  const url = `${SHOPIFY_BASE}${pathname}`;
  const res = await __withThrottle(() => fetch(url, {
    method,
    headers: {
      'X-Shopify-Access-Token': SHOPIFY_ADMIN_TOKEN,
      'Content-Type': 'application/json',
      ...headers
    },
    body: body ? JSON.stringify(body) : undefined
  }));

  if (res.status === 429 || (res.status >= 500 && res.status < 600)) {
    const retryAfterHeader = res.headers.get('Retry-After');
    const retryAfter = retryAfterHeader ? parseFloat(retryAfterHeader) * 1000 : Math.min(2000 * attempt, 10000);
    if (attempt <= 5) {
      console.warn(`Shopify ${res.status}. Retrying in ${retryAfter}ms (attempt ${attempt})...`);
      await new Promise(r => setTimeout(r, retryAfter));
      return shopifyFetch(pathname, { method, headers, body }, attempt + 1);
    }
  }

  if (!res.ok) {
    const t = await res.text().catch(() => '');
    throw new Error(`Shopify ${method} ${pathname} failed: ${res.status} ${res.statusText} - ${t}`);
  }
  return res.json();
}

async function findOrderByName(orderName) {
  const encoded = encodeURIComponent(orderName);
  const data = await shopifyFetch(`/orders.json?name=${encoded}&status=any`);
  const order = (data.orders || []).find(o => o?.name === orderName);
  if (!order) throw new Error(`Order not found: ${orderName}`);
  return order;
}

async function upsertOrderMetafield(orderId, namespace, key, value, typeHint) {
  const list = await shopifyFetch(`/orders/${orderId}/metafields.json`);
  const existing = (list.metafields || []).find(m => m.namespace === namespace && m.key === key);
  if (existing) {
    await shopifyFetch(`/metafields/${existing.id}.json`, {
      method: 'PUT',
      body: { metafield: { id: existing.id, value } }
    });
  } else {
    await shopifyFetch(`/orders/${orderId}/metafields.json`, {
      method: 'POST',
      body: {
        metafield: {
          namespace,
          key,
          type: typeHint || 'single_line_text_field',
          value
        }
      }
    });
  }
}

async function fetchOrderMetafields(orderId) {
  const data = await shopifyFetch(`/orders/${orderId}/metafields.json`);
  const map = {};
  for (const mf of (data.metafields || [])) {
    const ns = (mf.namespace || '').trim();
    const key = (mf.key || '').trim();
    const val = (mf.value ?? '').toString().trim();
    if (ns && key) map[`${ns}.${key}`] = val;
  }
  return map;
}

async function fetchOrderNote(orderId) {
  const data = await shopifyFetch(`/orders/${orderId}.json?fields=note`);
  return data?.order?.note || '';
}

async function updateOrderNote(orderId, note) {
  await shopifyFetch(`/orders/${orderId}.json`, {
    method: 'PUT',
    body: { order: { id: orderId, note } }
  });
}

/* =========================
   Arrange Logic
========================= */
// Given current value like "", "OHC", or "OHC & Bospeed", add a supplier
// and return the FINAL STRING we will attempt to set.
// If value already includes supplier, returns current unchanged.
function computeArrangedWith(current, supplier) {
  const cur = (current || '').trim();
  if (!cur) return supplier;
  // Normalize by splitting on "&"
  const parts = cur.split('&').map(s => s.trim()).filter(Boolean);
  if (parts.includes(supplier)) return cur;

  // Create a new set and build a normalized candidate string sorted A..Z
  const set = new Set(parts.concat([supplier]));
  const combined = Array.from(set).sort((a, b) => a.localeCompare(b)).join(' & ');
  return combined;
}

/* =========================
   Slack App
========================= */
const app = new App({
  token: SLACK_BOT_TOKEN,
  appToken: SLACK_APP_TOKEN,
  signingSecret: SLACK_SIGNING_SECRET,
  socketMode: true,
  processBeforeResponse: true
});

app.error((err) => {
  console.error('⚠️ Bolt error:', err?.stack || err?.message || err);
});

app.command('/ping', async ({ ack, respond }) => {
  await ack();
  await respond({ text: 'pong' });
});

/* =========================
   Posting & Actions
========================= */
function buildMessageBlocks({ docKey, docName, supplier, orderName, orderDate }) {
  const textLines = [
    `*New order detected* in _${docName}_`,
    `• *Supplier:* ${supplier}`,
    `• *Order #:* ${orderName}`,
    orderDate ? `• *Order Date:* ${orderDate}` : null
  ].filter(Boolean);

  return [
    {
      type: 'section',
      text: { type: 'mrkdwn', text: textLines.join('\n') }
    },
    {
      type: 'actions',
      elements: [
        {
          type: 'button',
          action_id: 'arrange_order',
          text: { type: 'plain_text', text: 'Arrange' },
          style: 'primary',
          value: JSON.stringify({ docKey, supplier, orderName, orderDate })
        },
        {
          type: 'button',
          action_id: 'ignore_order',
          text: { type: 'plain_text', text: 'Ignore' },
          value: JSON.stringify({ docKey, supplier, orderName, orderDate })
        }
      ]
    }
  ];
}

app.action('ignore_order', async ({ ack, body, client }) => {
  await ack();
  try {
    const payload = JSON.parse(body.actions?.[0]?.value || '{}');
    const channel = body.channel?.id || WATCH_CHANNEL_ID;
    const thread_ts = body.message?.ts;
    await client.chat.postMessage({
      channel,
      thread_ts,
      text: `Ignored ${payload.orderName} for ${payload.supplier}.`
    });
  } catch (e) {
    console.error('ignore_order failed:', e);
  }
});

app.action('arrange_order', async ({ ack, body, client }) => {
  await ack();
  try {
    const payload = JSON.parse(body.actions?.[0]?.value || '{}');
    const { supplier, orderName, orderDate } = payload;
    const channel = body.channel?.id || WATCH_CHANNEL_ID;
    const thread_ts = body.message?.ts;

    // 1) Find Shopify order
    const order = await findOrderByName(orderName);
    const orderId = order.id;

    // 2) Metafields
    const mf = await fetchOrderMetafields(orderId);

    // 2a) If arrange_status !== "Arranged", set to "Arranged"
    if ((mf['custom.arrange_status'] || '').trim() !== 'Arranged') {
      await upsertOrderMetafield(orderId, 'custom', 'arrange_status', 'Arranged');
    }

    // 3) Compute new custom._nc_arranged_with by "adding" supplier
    const current = (mf['custom._nc_arranged_with'] || '').trim();
    const nextValue = computeArrangedWith(current, supplier);

    // Attempt to set. If Shopify rejects (enum not allowed), throw helpful error.
    try {
      await upsertOrderMetafield(orderId, 'custom', '_nc_arranged_with', nextValue);
    } catch (err) {
      const msg = `Cannot set custom._nc_arranged_with="${nextValue}". Add this exact value to the allowed list in Shopify Admin, then retry.`;
      throw new Error(msg);
    }

    // 4) Prepend order note line
    const existingNote = await fetchOrderNote(orderId);
    const now = new Date();
    const mm = String(now.getMonth() + 1).padStart(2, '0');
    const dd = String(now.getDate()).padStart(2, '0');
    const yyyy = String(now.getFullYear());
    const headerLine = `Update ${mm}/${dd}/${yyyy}: Arranged with ${supplier}${orderDate ? ` on ${orderDate}` : ''}`;
    const dashLine = '————————————';
    const newNote = `${headerLine}\n${dashLine}\n${existingNote || ''}`;
    await updateOrderNote(orderId, newNote);

    await client.chat.postMessage({
      channel,
      thread_ts,
      text: `✅ Arranged ${orderName} with ${supplier}. Metafields and order note updated.`
    });
  } catch (e) {
    console.error('arrange_order failed:', e);
    const channel = body.channel?.id || WATCH_CHANNEL_ID;
    const thread_ts = body.message?.ts;
    if (channel) {
      await app.client.chat.postMessage({
        channel,
        thread_ts,
        text: `❌ Arrange failed: ${e?.message || e}`
      }).catch(() => {});
    }
  }
});

/* =========================
   Scanner (polling)
========================= */
async function scanOnceAndNotify() {
  const token = await getGraphToken();

  for (const cfg of SUPPLIER_DOCS) {
    const { key, name, shareUrl, sheet, colOrder, colDate, supplier } = cfg;
    const seenFile = path.join(SEEN_DIR, `${key}.json`);
    const seen = new Set(await readJsonSafe(seenFile, []));

    let driveItemId;
    try {
      driveItemId = await getDriveItemIdFromShare(shareUrl, token);
    } catch (e) {
      console.error(`[${key}] driveItemId error:`, e?.message || e);
      continue;
    }

    let values;
    try {
      values = await readWorksheetValues(driveItemId, sheet, token);
    } catch (e) {
      console.error(`[${key}] readWorksheetValues error:`, e?.message || e);
      continue;
    }

    const colOrderIdx = colLetterToIndex(colOrder);
    const colDateIdx = colLetterToIndex(colDate);

    const channel = WATCH_CHANNEL_ID;
    if (!channel) {
      console.error('WATCH_CHANNEL_ID is not set; cannot post notifications.');
      return;
    }

    // Walk rows, find new orders like C#XXXX (4+ digits) in the order column
    for (let r = 0; r < values.length; r++) {
      const row = values[r] || [];
      const orderCell = (row[colOrderIdx] ?? '').toString().trim();
      if (!/^C#\d{4,}$/.test(orderCell)) continue;

      if (seen.has(orderCell)) continue; // already notified

      const orderDate = (row[colDateIdx] ?? '').toString().trim();
      const blocks = buildMessageBlocks({
        docKey: key,
        docName: name,
        supplier,
        orderName: orderCell,
        orderDate
      });

      try {
        await app.client.chat.postMessage({
          channel,
          text: `New order ${orderCell} detected in ${name}`,
          blocks
        });
        seen.add(orderCell);
      } catch (e) {
        console.error(`[${key}] Slack post failed for ${orderCell}:`, e?.message || e);
      }
    }

    // persist seen set
    await writeJsonAtomic(seenFile, Array.from(seen));
  }
}

/* =========================
   Start
========================= */
(async () => {
  // Lightweight connectivity checks (non-fatal)
  try {
    await shopifyFetch('/shop.json');
    console.log('[shopify] connectivity ok');
  } catch (e) {
    console.error('⚠️ Shopify check failed:', e?.message || e);
  }

  try {
    await app.start();
    console.log('[slack] app started (Socket Mode)');
  } catch (e) {
    console.error('Slack start failed:', e?.message || e);
    process.exit(1);
  }

  // Initial scan immediately, then interval
  try {
    await scanOnceAndNotify();
  } catch (e) {
    console.error('Initial scan failed:', e?.message || e);
  }

  const intervalMs = Math.max(15000, Number(SCAN_INTERVAL_MS) || 60000);
  setInterval(async () => {
    try {
      await scanOnceAndNotify();
    } catch (e) {
      console.error('Periodic scan failed:', e?.message || e);
    }
  }, intervalMs);
})();