const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const DATA_DIR = path.join(__dirname, 'workflow-data');
const AUDIT_LOG = path.join(DATA_DIR, 'audit-log.jsonl');
const FLAG_STATE = path.join(DATA_DIR, 'flag-state.json');

function ensureStore() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  if (!fs.existsSync(AUDIT_LOG)) fs.writeFileSync(AUDIT_LOG, '');
  if (!fs.existsSync(FLAG_STATE)) fs.writeFileSync(FLAG_STATE, '{}');
}

function readJson(file, fallback) {
  ensureStore();
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8') || 'null') ?? fallback;
  } catch {
    return fallback;
  }
}

function writeJson(file, value) {
  ensureStore();
  fs.writeFileSync(file, JSON.stringify(value, null, 2));
}

function readAuditLog() {
  ensureStore();
  return fs.readFileSync(AUDIT_LOG, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map(line => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

function hashRecord(record) {
  return crypto.createHash('sha256')
    .update(JSON.stringify(record))
    .digest('hex');
}

function appendAuditEntry(entry) {
  ensureStore();
  const previous = readAuditLog().at(-1)?.record_hash || null;
  const body = {
    id: crypto.randomUUID(),
    timestamp: new Date().toISOString(),
    previous_hash: previous,
    ...entry,
  };
  body.record_hash = hashRecord(body);
  fs.appendFileSync(AUDIT_LOG, `${JSON.stringify(body)}\n`);
  return body;
}

function latestStatusByEntity() {
  const status = new Map();
  for (const entry of readAuditLog()) {
    if (!entry.organization_id) continue;
    status.set(String(entry.organization_id), {
      current_review_status: entry.current_review_status || entry.action_type || 'Logged',
      reviewer: entry.user_identity || null,
      last_action_at: entry.timestamp,
      last_action_type: entry.action_type,
      last_note: entry.reviewer_note || '',
    });
  }
  return status;
}

function daysSince(iso) {
  const then = new Date(iso);
  if (Number.isNaN(then.getTime())) return 0;
  return Math.max(0, Math.floor((Date.now() - then.getTime()) / 86400000));
}

function seedFlagState(queue) {
  const state = readJson(FLAG_STATE, {});
  let changed = false;
  for (const row of queue) {
    const key = String(row.entity_id);
    if (!state[key]) {
      state[key] = {
        first_raised_at: new Date().toISOString(),
        first_flag_code: row.top_flag || 'REVIEW_SIGNAL',
      };
      changed = true;
    }
  }
  if (changed) writeJson(FLAG_STATE, state);
  return state;
}

function buildFlaggedNotActioned(queue) {
  const state = seedFlagState(queue);
  const latest = latestStatusByEntity();
  const completed = new Set(['Clear', 'Approve', 'Sign-off', 'Cleared', 'Approved', 'Signed off']);
  return queue
    .filter(row => {
      const last = latest.get(String(row.entity_id));
      return !last || !completed.has(last.current_review_status);
    })
    .map(row => {
      const flag = state[String(row.entity_id)] || {};
      const last = latest.get(String(row.entity_id));
      return {
        ...row,
        flag_first_raised_at: flag.first_raised_at || new Date().toISOString(),
        days_since_raised: daysSince(flag.first_raised_at || new Date().toISOString()),
        current_review_status: last?.current_review_status || 'Flagged - not actioned',
        reviewer: last?.reviewer || 'Unassigned',
        last_action_at: last?.last_action_at || null,
      };
    })
    .sort((a, b) =>
      Number(b.total_external_public_funding || 0) - Number(a.total_external_public_funding || 0) ||
      Number(b.score || 0) - Number(a.score || 0)
    );
}

function auditTrailForEntity(entityId) {
  return readAuditLog()
    .filter(entry => String(entry.organization_id) === String(entityId))
    .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
}

function createWorkflowStore() {
  ensureStore();
  return {
    appendAuditEntry,
    readAuditLog,
    auditTrailForEntity,
    buildFlaggedNotActioned,
    hashRecord,
  };
}

module.exports = { createWorkflowStore };
