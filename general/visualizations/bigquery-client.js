const { execFileSync } = require('child_process');

class BigQueryClient {
  constructor(options = {}) {
    this.projectId = options.projectId || process.env.BIGQUERY_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT || 'agency2026ot-bco-0429';
    this.location = options.location || process.env.BIGQUERY_LOCATION || 'northamerica-northeast1';
  }

  async getAccessToken() {
    if (process.env.BIGQUERY_ACCESS_TOKEN) return process.env.BIGQUERY_ACCESS_TOKEN;
    try {
      return execFileSync('gcloud', ['auth', 'print-access-token'], {
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'ignore'],
      }).trim();
    } catch (error) {
      throw new Error('BigQuery auth unavailable. Set BIGQUERY_ACCESS_TOKEN or run `gcloud auth application-default login` / `gcloud auth login`.');
    }
  }

  async query(sql, params = {}) {
    if (!this.projectId) {
      throw new Error('BIGQUERY_PROJECT_ID or GOOGLE_CLOUD_PROJECT is required for BigQuery backend.');
    }
    const accessToken = await this.getAccessToken();
    const parameterMode = Object.keys(params).length ? 'NAMED' : undefined;
    const queryParameters = Object.entries(params).map(([name, value]) => ({
      name,
      parameterType: inferParameterType(value),
      parameterValue: encodeParameterValue(value),
    }));

    const response = await fetch(`https://bigquery.googleapis.com/bigquery/v2/projects/${encodeURIComponent(this.projectId)}/queries`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query: sql,
        useLegacySql: false,
        location: this.location,
        parameterMode,
        queryParameters,
      }),
    });

    const payload = await response.json();
    if (!response.ok || payload.error) {
      const message = payload.error?.message || JSON.stringify(payload);
      throw new Error(`BigQuery query failed: ${message}`);
    }
    return decodeRows(payload.schema?.fields || [], payload.rows || []);
  }
}

function inferParameterType(value) {
  if (typeof value === 'number' && Number.isInteger(value)) return { type: 'INT64' };
  if (typeof value === 'number') return { type: 'FLOAT64' };
  if (typeof value === 'boolean') return { type: 'BOOL' };
  return { type: 'STRING' };
}

function encodeParameterValue(value) {
  return { value: value === null || value === undefined ? null : String(value) };
}

function decodeValue(field, cell) {
  const value = cell?.v;
  if (value === null || value === undefined) return null;
  if (field.type === 'RECORD' && Array.isArray(field.fields)) {
    if (field.mode === 'REPEATED') {
      return (value || []).map(item => decodeRows(field.fields, [item.v ? item : { f: item.f || [] }])[0]);
    }
    return decodeRows(field.fields, [{ f: value.f || [] }])[0];
  }
  if (field.mode === 'REPEATED') return (value || []).map(v => v.v ?? v);
  if (field.type === 'TIMESTAMP') {
    const seconds = Number(value);
    return Number.isFinite(seconds) ? new Date(seconds * 1000).toISOString() : value;
  }
  if (field.type === 'DATE' || field.type === 'DATETIME' || field.type === 'TIME') return value;
  if (field.type === 'INTEGER' || field.type === 'INT64') return Number(value);
  if (field.type === 'FLOAT' || field.type === 'FLOAT64' || field.type === 'NUMERIC' || field.type === 'BIGNUMERIC') return Number(value);
  if (field.type === 'BOOLEAN' || field.type === 'BOOL') return value === 'true' || value === true;
  return value;
}

function decodeRows(fields, rows) {
  return rows.map(row => {
    const out = {};
    fields.forEach((field, i) => {
      out[field.name] = decodeValue(field, row.f?.[i]);
    });
    return out;
  });
}

function tableRef(envName, fallback) {
  const configured = process.env[envName] || fallback;
  if (!configured) throw new Error(`${envName} is required for BigQuery backend.`);
  if (configured.startsWith('`') && configured.endsWith('`')) return configured;
  return `\`${configured}\``;
}

module.exports = { BigQueryClient, tableRef };
