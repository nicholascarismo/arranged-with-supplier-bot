import 'dotenv/config';
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import boltPkg from '@slack/bolt';

const { App } = boltPkg;

/* =========================
   Env & Config
========================= */
const {
  SLACK_BOT_TOKEN,
  SLACK_APP_TOKEN,
  SLACK_SIGNING_SECRET,

  SHOPIFY_DOMAIN,
  SHOPIFY_ADMIN_TOKEN,
  SHOPIFY_API_VERSION = '2025-01',

  WATCH_CHANNEL_ID
} = process.env;

const {
  TRELLO_KEY,
  TRELLO_TOKEN,
  TRELLO_BOARD_ID,
  TRELLO_LIST_ID
} = process.env;

function mustHave(varName) {
  if (!process.env[varName]) {
    console.error(`Missing required env: ${varName}`);
    process.exit(1);
  }
}
mustHave('SLACK_BOT_TOKEN');
mustHave('SLACK_APP_TOKEN');
mustHave('SHOPIFY_DOMAIN');
mustHave('SHOPIFY_ADMIN_TOKEN');

/* =========================
   Paths & Persistence
========================= */
const DATA_DIR = path.resolve('./data');
const RUN_LOG = path.join(DATA_DIR, 'runs.json');
const SUPPLIERS_FILE = path.resolve('./suppliers.json');

async function ensureDataDir() {
  await fsp.mkdir(DATA_DIR, { recursive: true });
}

async function writeJsonAtomic(filePath, data) {
  const tmp = `${filePath}.tmp-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  await fsp.writeFile(tmp, JSON.stringify(data, null, 2), 'utf8');
  await fsp.rename(tmp, filePath);
}

async function readJsonSafe(filePath, fallback = null) {
  try {
    const txt = await fsp.readFile(filePath, 'utf8');
    return JSON.parse(txt);
  } catch {
    return fallback;
  }
}

/* =========================
   Suppliers loader
========================= */
async function loadSuppliers() {
  try {
    const txt = await fsp.readFile(SUPPLIERS_FILE, 'utf8');
    const arr = JSON.parse(txt);
    if (!Array.isArray(arr)) throw new Error('suppliers.json must be a JSON array of strings');
    const cleaned = Array.from(new Set(arr.map(s => (typeof s === 'string' ? s.trim() : '')).filter(Boolean)));
    if (!cleaned.length) throw new Error('suppliers.json list is empty');
    return cleaned;
  } catch (err) {
    console.error('Failed to read suppliers.json:', err?.message || err);
    // Fallback: at least OHC
    return ['OHC'];
  }
}

/* =========================
   Trello helpers
========================= */
async function trelloPOST(pathname, payload) {
  if (!TRELLO_KEY || !TRELLO_TOKEN) throw new Error('Missing TRELLO_KEY/TRELLO_TOKEN');
  if (!TRELLO_LIST_ID && !TRELLO_BOARD_ID) throw new Error('Missing TRELLO_LIST_ID or TRELLO_BOARD_ID');
  const url = `https://api.trello.com/1${pathname}?key=${encodeURIComponent(TRELLO_KEY)}&token=${encodeURIComponent(TRELLO_TOKEN)}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload || {})
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`Trello POST ${pathname} -> ${res.status} ${res.statusText} ${text}`);
  }
  return res.json();
}

async function createTrelloCardSimple(title) {
  // Prefer explicit LIST ID from env
  if (TRELLO_LIST_ID) {
    return trelloPOST('/cards', { idList: TRELLO_LIST_ID, name: title });
  }
  // If only BOARD is provided (rare), create on first open list
  if (TRELLO_BOARD_ID) {
    // Get lists on the board (open)
    const url = `https://api.trello.com/1/boards/${encodeURIComponent(TRELLO_BOARD_ID)}/lists?filter=open&key=${encodeURIComponent(TRELLO_KEY)}&token=${encodeURIComponent(TRELLO_TOKEN)}`;
    const r = await fetch(url);
    if (!r.ok) throw new Error(`Trello GET lists -> ${r.status}`);
    const lists = await r.json();
    if (!Array.isArray(lists) || !lists.length) throw new Error('No open lists on the Trello board');
    const idList = lists[0].id;
    return trelloPOST('/cards', { idList, name: title });
  }
  throw new Error('Cannot resolve Trello list/board');
}

/* =========================
   Shopify Helpers
========================= */
const SHOPIFY_BASE = `https://${SHOPIFY_DOMAIN}/admin/api/${SHOPIFY_API_VERSION}`;
let __gate = Promise.resolve();
const __MIN_GAP_MS = 400; // polite global throttle

async function withThrottle(fn) {
  const prev = __gate;
  let release;
  __gate = new Promise(res => { release = res; });
  await prev;
  try {
    return await fn();
  } finally {
    setTimeout(release, __MIN_GAP_MS);
  }
}

async function shopifyFetch(pathname, { method = 'GET', headers = {}, body } = {}, attempt = 1) {
  const url = `${SHOPIFY_BASE}${pathname}`;
  const res = await withThrottle(() =>
    fetch(url, {
      method,
      headers: {
        'X-Shopify-Access-Token': SHOPIFY_ADMIN_TOKEN,
        'Content-Type': 'application/json',
        ...headers
      },
      body: body ? JSON.stringify(body) : undefined
    })
  );

  if (res.status === 429 || (res.status >= 500 && res.status < 600)) {
    const retryAfterHeader = res.headers.get('Retry-After');
    const retryAfter = retryAfterHeader ? parseFloat(retryAfterHeader) * 1000 : Math.min(2000 * attempt, 10000);
    if (attempt <= 5) {
      console.warn(`Shopify ${res.status} on ${pathname}. Retrying in ${retryAfter}ms (attempt ${attempt})`);
      await new Promise(r => setTimeout(r, retryAfter));
      return shopifyFetch(pathname, { method, headers, body }, attempt + 1);
    }
  }

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`Shopify ${method} ${pathname} failed: ${res.status} ${res.statusText} - ${text}`);
  }
  const ct = res.headers.get('content-type') || '';
  if (ct.includes('application/json')) return res.json();
  return res.text();
}

// Find by exact "name" e.g. "C#1234"
async function findOrderByName(orderName) {
  const enc = encodeURIComponent(orderName);
  const data = await shopifyFetch(`/orders.json?name=${enc}&status=any`);
  const order = (data.orders || []).find(o => o.name === orderName);
  if (!order) throw new Error(`Order ${orderName} not found`);
  return order;
}

async function fetchOrderMetafields(orderId) {
  const data = await shopifyFetch(`/orders/${orderId}/metafields.json`);
  const out = {};
  for (const m of (data.metafields || [])) {
    const ns = (m.namespace || '').trim();
    const key = (m.key || '').trim();
    const val = (m.value ?? '').toString().trim();
    if (ns && key) out[`${ns}.${key}`] = { id: m.id, value: val };
  }
  return out;
}

// Update an existing metafield (by id)
async function updateMetafieldById(id, value) {
  return shopifyFetch(`/metafields/${id}.json`, {
    method: 'PUT',
    body: { metafield: { id, value } }
  });
}

// Create a new order metafield
async function createOrderMetafield(orderId, namespace, key, value, typeHint = 'single_line_text_field') {
  return shopifyFetch(`/orders/${orderId}/metafields.json`, {
    method: 'POST',
    body: { metafield: { namespace, key, type: typeHint, value } }
  });
}

async function upsertOrderMetafield(orderId, namespace, key, value, typeHint) {
  const all = await fetchOrderMetafields(orderId);
  const existing = all[`${namespace}.${key}`];
  if (!existing) {
    return createOrderMetafield(orderId, namespace, key, value, typeHint);
  }
  if (existing.value === value) return { ok: true, unchanged: true };
  return updateMetafieldById(existing.id, value);
}

async function fetchOrderCore(orderId) {
  // Get note and created_at in one call
  const data = await shopifyFetch(`/orders/${orderId}.json?fields=note,created_at`);
  return data?.order || {};
}

async function updateOrderNote(orderId, note) {
  return shopifyFetch(`/orders/${orderId}.json`, {
    method: 'PUT',
    body: { order: { id: orderId, note } }
  });
}

// Read current tags as an array
async function fetchOrderTags(orderId) {
  const data = await shopifyFetch(`/orders/${orderId}.json?fields=tags`);
  const raw = data?.order?.tags || '';
  return raw.split(',').map(t => t.trim()).filter(Boolean);
}

// Replace full tag set for an order
async function updateOrderTags(orderId, tagsArray) {
  const tags = tagsArray.join(', ');
  return shopifyFetch(`/orders/${orderId}.json`, {
    method: 'PUT',
    body: { order: { id: orderId, tags } }
  });
}

/* =========================
   _nc_arranged_with logic
   - Future-proof for multiple suppliers (A & B & C ...)
   - Try candidate strings until Shopify accepts; else throw
========================= */
function parseSuppliersFromValue(val) {
  const s = (val || '').trim();
  if (!s) return [];
  return s.split('&').map(x => x.trim()).filter(Boolean);
}

function uniqPreserve(arr) {
  const seen = new Set();
  const out = [];
  for (const v of arr) {
    if (!seen.has(v)) { seen.add(v); out.push(v); }
  }
  return out;
}

// generate candidate strings to try (ordered by most natural)
function* generateCombinationCandidates(existingList, newSupplier) {
  const base = uniqPreserve([...existingList, newSupplier]);

  // 1) Keep existing order, append newSupplier at the end (if it wasn't there)
  yield base.join(' & ');

  // 2) Alphabetical order
  const alpha = [...base].sort((a, b) => a.localeCompare(b));
  yield alpha.join(' & ');

  // 3) If there are only 2-3 suppliers, try all permutations
  if (base.length <= 3) {
    const permute = (arr) => {
      if (arr.length <= 1) return [arr];
      const out = [];
      for (let i = 0; i < arr.length; i++) {
        const rest = arr.slice(0, i).concat(arr.slice(i + 1));
        for (const tail of permute(rest)) out.push([arr[i], ...tail]);
      }
      return out;
    };
    for (const p of permute(base)) {
      yield p.join(' & ');
    }
  }
}

/* =========================
   Slack App (Socket Mode)
========================= */
const app = new App({
  token: SLACK_BOT_TOKEN,
  appToken: SLACK_APP_TOKEN,
  signingSecret: SLACK_SIGNING_SECRET,
  socketMode: true,
  processBeforeResponse: true
});

app.error((e) => {
  console.error('⚠️ Bolt error:', e?.stack || e?.message || e);
});

/* =========================
   /ping -> pong
========================= */
app.command('/ping', async ({ ack, respond, command }) => {
  await ack();
  const where = command?.channel_id ? `<#${command.channel_id}>` : 'here';
  await respond({ text: `pong (${where})` });
});

/* =========================
   /res-inv -> Reserve Arranged Inventory (single order)
========================= */
app.command('/res-inv', async ({ ack, body, client, logger }) => {
  await ack();

  try {
    const suppliers = await loadSuppliers();
    const defaultSupplier = suppliers.includes('OHC') ? 'OHC' : suppliers[0];
    const supplierOptions = suppliers.slice(0, 100).map(s => ({
      text: { type: 'plain_text', text: s },
      value: s
    }));

    await client.views.open({
      trigger_id: body.trigger_id,
      view: {
        type: 'modal',
        callback_id: 'res_inv_modal_submit',
        private_metadata: JSON.stringify({ channel: body.channel_id || WATCH_CHANNEL_ID || '' }),
        title: { type: 'plain_text', text: 'Reserve Arranged Inv' },
        submit: { type: 'plain_text', text: 'Apply' },
        close: { type: 'plain_text', text: 'Cancel' },
        blocks: [
          {
            type: 'input',
            block_id: 'order_block',
            label: { type: 'plain_text', text: 'Order # (must be like C#4352)' },
            element: {
              type: 'plain_text_input',
              action_id: 'order_input',
              placeholder: { type: 'plain_text', text: 'e.g., C#5723' }
            }
          },
          {
            type: 'input',
            block_id: 'inv_block',
            label: { type: 'plain_text', text: 'INV #' },
            element: {
              type: 'plain_text_input',
              action_id: 'inv_input',
              placeholder: { type: 'plain_text', text: 'e.g., INV3952' }
            }
          },
          {
            type: 'input',
            block_id: 'supplier_block',
            label: { type: 'plain_text', text: 'Supplier' },
            element: {
              type: 'static_select',
              action_id: 'supplier_select',
              initial_option: { text: { type: 'plain_text', text: defaultSupplier }, value: defaultSupplier },
              options: supplierOptions
            }
          },
          {
            type: 'input',
            block_id: 'invoiced_block',
            label: { type: 'plain_text', text: 'Is the INV item already invoiced?' },
            element: {
              type: 'radio_buttons',
              action_id: 'invoiced_choice',
              initial_option: { text: { type: 'plain_text', text: 'No' }, value: 'No' },
              options: [
                { text: { type: 'plain_text', text: 'No' }, value: 'No' },
                { text: { type: 'plain_text', text: 'Yes' }, value: 'Yes' }
              ]
            }
          },
          {
            type: 'input',
            block_id: 'invoice_date_block',
            label: { type: 'plain_text', text: 'Invoice Date (MM-YY)' },
            optional: true,
            element: {
              type: 'plain_text_input',
              action_id: 'invoice_date_input',
              placeholder: { type: 'plain_text', text: 'e.g., 10-23' }
            }
          }
        ]
      }
    });
  } catch (e) {
    logger.error('open /res-inv modal failed:', e);
  }
});

/* =========================
   /arrange -> open modal with 5 sets
========================= */
app.command('/arrange', async ({ ack, body, client, logger }) => {
  await ack();
  try {
    const suppliers = await loadSuppliers();
    const defaultSupplier = suppliers.includes('OHC') ? 'OHC' : suppliers[0];
    const supplierOptions = suppliers.slice(0, 100).map(s => ({
      text: { type: 'plain_text', text: s },
      value: s
    }));

    const makeSet = (idx) => ([
      {
        type: 'input',
        block_id: `order_block_${idx}`,
        label: { type: 'plain_text', text: `#${idx} — Order # (must start with C#)` },
        element: {
          type: 'plain_text_input',
          action_id: `order_input_${idx}`,
          placeholder: { type: 'plain_text', text: 'e.g., C#5723' }
        },
        optional: true
      },
      {
        type: 'input',
        block_id: `supplier_block_${idx}`,
        label: { type: 'plain_text', text: `#${idx} — Supplier` },
        element: {
          type: 'static_select',
          action_id: `supplier_select_${idx}`,
          initial_option: { text: { type: 'plain_text', text: defaultSupplier }, value: defaultSupplier },
          options: supplierOptions
        },
        optional: false
      },
      { type: 'divider' }
    ]);

    const blocks = [
      { type: 'header', text: { type: 'plain_text', text: 'Arrange Orders' } },
      { type: 'section', text: { type: 'mrkdwn', text: 'Fill any of the sets below. Only sets with an Order # will be processed.' } },
      { type: 'divider' },
      ...makeSet(1),
      ...makeSet(2),
      ...makeSet(3),
      ...makeSet(4),
      ...makeSet(5)
    ];

    await client.views.open({
      trigger_id: body.trigger_id,
      view: {
        type: 'modal',
        callback_id: 'arrange_modal_submit',
        private_metadata: JSON.stringify({ channel: body.channel_id || WATCH_CHANNEL_ID || '' }),
        title: { type: 'plain_text', text: 'Arrange' },
        submit: { type: 'plain_text', text: 'Apply' },
        close: { type: 'plain_text', text: 'Cancel' },
        blocks
      }
    });
  } catch (e) {
    logger.error(e);
  }
});

/* =========================
   /res-inv modal -> submission
========================= */
app.view('res_inv_modal_submit', async ({ ack, body, view, client, logger }) => {
  const state = view.state.values || {};

  const orderVal = (state?.order_block?.order_input?.value || '').trim();
  const invNumber = (state?.inv_block?.inv_input?.value || '').trim();
  const supplierVal = state?.supplier_block?.supplier_select?.selected_option?.value || '';
  const invoicedChoice = state?.invoiced_block?.invoiced_choice?.selected_option?.value || 'No';
  const invoiceDate = (state?.invoice_date_block?.invoice_date_input?.value || '').trim();

  const suppliers = await loadSuppliers();

  // Validate
  const errors = {};

  if (!/^C#\d{1,6}$/i.test(orderVal)) {
    errors['order_block'] = 'Order # must start with "C#" followed by digits (e.g., C#1234).';
  }
  if (!invNumber) {
    errors['inv_block'] = 'INV # is required.';
  }
  if (!suppliers.includes(supplierVal)) {
    errors['supplier_block'] = `Supplier must be one of: ${suppliers.join(', ')}`;
  }
  if (invoicedChoice === 'Yes') {
    if (!/^\d{2}-\d{2}$/.test(invoiceDate)) {
      errors['invoice_date_block'] = 'Invoice Date is required in format MM-YY (e.g., 10-23).';
    }
  }

  if (Object.keys(errors).length) {
    await ack({ response_action: 'errors', errors });
    return;
  }

  await ack();

  const md = JSON.parse(view.private_metadata || '{}');
  const channel = md.channel || WATCH_CHANNEL_ID || body?.user?.id;

  // Parent message
  let parent;
  try {
    parent = await client.chat.postMessage({
      channel,
      text: `Reserving arranged inventory for ${orderVal}…`
    });
  } catch (e) {
    logger.error('post parent failed (/res-inv):', e);
  }
  const thread_ts = parent?.ts;

  try {
    const result = await reserveIncomingOne({
      orderName: orderVal,
      invNumber,
      supplier: supplierVal,
      invoiced: invoicedChoice === 'Yes',
      invoiceDate
    });

    await client.chat.postMessage({
      channel,
      thread_ts,
      text: `✅ Reserved arranged inventory for ${orderVal} (${supplierVal}${invoicedChoice === 'Yes' ? `, invoiced ${invoiceDate}` : ''}).`
    });
  } catch (e) {
    logger.error('reserveIncomingOne failed:', e);
    await client.chat.postMessage({
      channel,
      thread_ts,
      text: `❌ ${orderVal} failed: ${e?.message || String(e)}`
    });
  }
});

/* =========================
   Modal submission handler
========================= */
app.view('arrange_modal_submit', async ({ ack, body, view, client, logger }) => {
  // Build list of entries
  const state = view.state.values || {};
  const entries = [];
  for (let i = 1; i <= 5; i++) {
    const orderVal = state?.[`order_block_${i}`]?.[`order_input_${i}`]?.value?.trim() || '';
    const supplierVal = state?.[`supplier_block_${i}`]?.[`supplier_select_${i}`]?.selected_option?.value || '';
    if (orderVal) entries.push({ idx: i, order: orderVal, supplier: supplierVal });
  }

  // Validation
  const errors = {};
  const orderSet = new Set();
  const suppliers = await loadSuppliers();

  for (const e of entries) {
    // order number must start with C#
    if (!/^C#\d{1,6}$/i.test(e.order)) {
      errors[`order_block_${e.idx}`] = 'Order # must start with "C#" followed by digits (e.g., C#1234).';
    }
    // supplier must be in list
    if (!suppliers.includes(e.supplier)) {
      errors[`supplier_block_${e.idx}`] = `Supplier must be one of: ${suppliers.join(', ')}`;
    }
    // no duplicates
    const key = `${e.order}`;
    if (orderSet.has(key)) {
      errors[`order_block_${e.idx}`] = 'Duplicate order in this submission.';
    } else {
      orderSet.add(key);
    }
  }

  if (Object.keys(errors).length) {
    await ack({ response_action: 'errors', errors });
    return;
  }

  await ack();

  const md = JSON.parse(view.private_metadata || '{}');
  const channel = md.channel || WATCH_CHANNEL_ID || body?.user?.id;

  // Post a parent message summarizing intent
  let parent;
  try {
    parent = await client.chat.postMessage({
      channel,
      text: `Arranging ${entries.length} order(s)…`
    });
  } catch (e) {
    logger.error('post parent failed:', e);
  }
  const thread_ts = parent?.ts;

  // Process entries sequentially to be gentle on Shopify
  const results = [];
  for (const e of entries) {
    try {
      const res = await arrangeOne(e.order, e.supplier);
      results.push({ ...e, ok: true, info: res });
      if (thread_ts) {
        await client.chat.postMessage({
          channel,
          thread_ts,
          text: `✅ ${e.order} arranged with ${e.supplier}`
        });
      }
    } catch (err) {
      results.push({ ...e, ok: false, error: err?.message || String(err) });
      if (thread_ts) {
        await client.chat.postMessage({
          channel,
          thread_ts,
          text: `❌ ${e.order} failed: ${err?.message || String(err)}`
        });
      }
    }
  }

  // Persist run log
  try {
    await ensureDataDir();
    const log = (await readJsonSafe(RUN_LOG, [])) || [];
    log.push({
      at: new Date().toISOString(),
      user: body?.user?.id || 'unknown',
      count: entries.length,
      results
    });
    while (log.length > 200) log.shift();
    await writeJsonAtomic(RUN_LOG, log);
  } catch (e) {
    logger.error('persist log failed:', e);
  }

  // Final summary
  try {
    const ok = results.filter(r => r.ok).map(r => r.order);
    const bad = results.filter(r => !r.ok).map(r => `${r.order} (${r.error})`);
    const lines = [];
    if (ok.length) lines.push(`✅ Done: ${ok.join(', ')}`);
    if (bad.length) lines.push(`❌ Failed: ${bad.join(', ')}`);
    await client.chat.postMessage({
      channel,
      thread_ts,
      text: lines.join('\n') || 'Done.'
    });
  } catch (e) {
    logger.error('post summary failed:', e);
  }
});

/* =========================
   Reserve Arranged Inventory worker (single order)
========================= */
async function reserveIncomingOne({ orderName, invNumber, supplier, invoiced, invoiceDate }) {
  // 1) Lookup order
  const order = await findOrderByName(orderName);
  const orderId = order.id;

  // 2) Fetch metafields once
  const mfsBefore = await fetchOrderMetafields(orderId);

  // 3) custom._nc_reserve_incoming -> "RESERVED INCOMING INVENTORY"
  await upsertOrderMetafield(orderId, 'custom', 'nc_reserve_incoming', 'RESERVED INCOMING INVENTORY', 'single_line_text_field');

  // 4) custom.reserved_inv_number_s_ -> invNumber
  await upsertOrderMetafield(orderId, 'custom', 'reserved_inv_number_s_', invNumber, 'multi_line_text_field');

  // 5) custom.arrange_status -> ensure "Arranged" (do nothing if already "Arranged")
  const currentArrange = (mfsBefore['custom.arrange_status']?.value || '').trim();
  if (currentArrange !== 'Arranged') {
    await upsertOrderMetafield(orderId, 'custom', 'arrange_status', 'Arranged', 'single_line_text_field');
  }

  // 6) custom._nc_arranged_with -> add supplier using allowed-value combinations
  const currentWith = (mfsBefore['custom._nc_arranged_with']?.value || '').trim();
  const list = parseSuppliersFromValue(currentWith);
  if (!list.includes(supplier)) {
    // Re-fetch current metafield id/value in case it changed since mfsBefore
    const mfsNow = await fetchOrderMetafields(orderId);
    const existing = mfsNow['custom._nc_arranged_with'];
    let success = false;
    let lastErr = null;

    for (const candidate of generateCombinationCandidates(parseSuppliersFromValue(existing?.value || ''), supplier)) {
      try {
        if (existing?.id) {
          await updateMetafieldById(existing.id, candidate);
        } else {
          await createOrderMetafield(orderId, 'custom', '_nc_arranged_with', candidate, 'single_line_text_field');
        }
        success = true;
        break;
      } catch (e) {
        // Shopify 422 if candidate not allowed; try next candidate
        lastErr = e;
      }
    }

    if (!success) {
      const tried = [];
      for (const c of generateCombinationCandidates(parseSuppliersFromValue(existing?.value || ''), supplier)) tried.push(c);
      throw new Error(
        `No allowed value found for custom._nc_arranged_with to include: ${[...parseSuppliersFromValue(existing?.value || ''), supplier].join(' & ')}. ` +
        `Tried: ${tried.join(' | ')}. Add the required combination to the metafield definition in Shopify Admin and retry.`
      );
    }
  }

  // 7) Tags: remove ArrangeStatus_NeedToArrange, add ArrangeStatus_Arranged
  try {
    const currentTags = await fetchOrderTags(orderId);
    const next = currentTags.filter(t => t !== 'ArrangeStatus_NeedToArrange');
    if (!next.includes('ArrangeStatus_Arranged')) next.push('ArrangeStatus_Arranged');
    const changed = next.length !== currentTags.length || next.some((t, i) => t !== currentTags[i]);
    if (changed) {
      await updateOrderTags(orderId, next);
    }
  } catch (e) {
    console.error(`⚠️ Tag update failed for ${orderName} (${orderId}):`, e?.message || e);
  }

  // 8) custom.parts_other -> append "Reserved [INV #]" (or set if blank)
  const partsOther = (mfsBefore['custom.parts_other']?.value || '').trim();
  const appendText = `Reserved ${invNumber}`;
  let nextParts;
  if (!partsOther) {
    nextParts = appendText;
  } else if (partsOther.includes(appendText)) {
    nextParts = partsOther; // avoid duplicate
  } else {
    nextParts = `${partsOther}; ${appendText}`;
  }
  if (nextParts !== partsOther) {
    if (mfsBefore['custom.parts_other']?.id) {
      await updateMetafieldById(mfsBefore['custom.parts_other'].id, nextParts);
    } else {
      await createOrderMetafield(orderId, 'custom', 'parts_other', nextParts, 'single_line_text_field');
    }
  }

  // 9) Prepend order note with exact format (no extra blank lines around dash line)
  const core = await fetchOrderCore(orderId);
  const existingNote = core?.note || '';
  const todayStr = formatMDY(new Date());
  const headerLine = `Update ${todayStr}: Reserved Arranged Inventory: ${invNumber} from ${supplier}`;
  const dashLine = '————————————';
  const newNote = `${headerLine}\n${dashLine}\n${existingNote || ''}`;
  await updateOrderNote(orderId, newNote);

  // 10) If invoiced == Yes, create two Trello cards
  if (invoiced) {
    // Titles:
    //  - Run ‘/invoice-review’ for [Order #], [Supplier] [MM-YY] Invoice
    //  - Add [Order #] to [Supplier] [MM-YY] bookmark folder
    const t1 = `Run ‘/invoice-review’ for ${orderName}, ${supplier} ${invoiceDate} Invoice`;
    const t2 = `Add ${orderName} to ${supplier} ${invoiceDate} bookmark folder`;

    try {
      await createTrelloCardSimple(t1);
    } catch (e) {
      console.error('⚠️ Trello card create failed (invoice-review):', e?.message || e);
    }
    try {
      await createTrelloCardSimple(t2);
    } catch (e) {
      console.error('⚠️ Trello card create failed (bookmark folder):', e?.message || e);
    }
  }

  return { orderId, orderName, invNumber, supplier, invoiced, invoiceDate };
}

/* =========================
   Core worker for one order
========================= */
function formatMDY(d) {
  const dt = new Date(d);
  const mm = String(dt.getMonth() + 1).padStart(2, '0');
  const dd = String(dt.getDate()).padStart(2, '0');
  const yyyy = String(dt.getFullYear());
  return `${mm}/${dd}/${yyyy}`;
}

async function arrangeOne(orderName, supplierToAdd) {
  // 1) Lookup order
  const order = await findOrderByName(orderName);
  const orderId = order.id;

  // 2) arrange_status -> ensure "Arranged"
  const mfsBefore = await fetchOrderMetafields(orderId);
  const arrangeMf = mfsBefore['custom.arrange_status'];
  const currentArrange = arrangeMf?.value || '';
  if (currentArrange !== 'Arranged') {
    await upsertOrderMetafield(orderId, 'custom', 'arrange_status', 'Arranged', 'single_line_text_field');
  }

  // 3) _nc_arranged_with -> add supplier by trying allowed strings
  const currentWith = (mfsBefore['custom._nc_arranged_with']?.value || '').trim();
  const list = parseSuppliersFromValue(currentWith);
  if (!list.includes(supplierToAdd)) {
    const candidates = generateCombinationCandidates(list, supplierToAdd);
    let success = false;
    const existing = mfsBefore['custom._nc_arranged_with'];
    let lastErr = null;

    for (const candidate of candidates) {
      try {
        if (existing?.id) {
          await updateMetafieldById(existing.id, candidate);
        } else {
          await createOrderMetafield(orderId, 'custom', '_nc_arranged_with', candidate, 'single_line_text_field');
        }
        success = true;
        break;
      } catch (e) {
        // Shopify will 422 if candidate is not in allowed list
        lastErr = e;
      }
    }

    if (!success) {
      const tried = [];
      const again = generateCombinationCandidates(list, supplierToAdd);
      for (const c of again) tried.push(c);
      throw new Error(
        `No allowed value found for custom._nc_arranged_with to include: ${[...list, supplierToAdd].join(' & ')}. ` +
        `Tried: ${tried.join(' | ')}. Add the required combination to the metafield definition in Shopify Admin and retry.`
      );
    }
  }

  // 4) Prepend order note
  const core = await fetchOrderCore(orderId);
  const existingNote = core?.note || '';
  const orderCreated = core?.created_at || order?.created_at || new Date().toISOString();

  const todayStr = formatMDY(new Date());
  const orderDateStr = formatMDY(orderCreated);

  const headerLine = `Update ${todayStr}: Arranged with ${supplierToAdd}`;
  const dashLine = '————————————';
  const newNote = `${headerLine}\n${dashLine}\n${existingNote || ''}`;

  await updateOrderNote(orderId, newNote);

// 5) Tags: remove ArrangeStatus_NeedToArrange, add ArrangeStatus_Arranged
try {
  const current = await fetchOrderTags(orderId);
  const next = current.filter(t => t !== 'ArrangeStatus_NeedToArrange');
  if (!next.includes('ArrangeStatus_Arranged')) next.push('ArrangeStatus_Arranged');
  // Only write if changed
  const changed = next.length !== current.length || next.some((t, i) => t !== current[i]);
  if (changed) {
    await updateOrderTags(orderId, next);
  }
} catch (e) {
  // Non-fatal: log but do not fail the arrange flow
  console.error(`⚠️ Tag update failed for ${orderName} (${orderId}):`, e?.message || e);
}

  return { orderId, orderName, supplier: supplierToAdd };
}

/* =========================
   Start app
========================= */
(async () => {
  await ensureDataDir();

  // Light connectivity checks (non-fatal)
  try {
    await shopifyFetch('/shop.json');
    console.log('[shopify] connectivity ok');
  } catch (e) {
    console.error('⚠️ Shopify check failed:', e?.message || e);
  }

  await app.start();
  console.log('[slack] app started (Socket Mode)');

  if (WATCH_CHANNEL_ID) {
    console.log(`[info] default channel set: ${WATCH_CHANNEL_ID}`);
  }
})();