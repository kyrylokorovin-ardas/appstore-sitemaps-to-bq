import crypto from "node:crypto";
import { BigQuery } from "@google-cloud/bigquery";

export function createBigQueryClient() {
  const projectId = process.env.GCP_PROJECT || "esoteric-parsec-147012";
  return new BigQuery({ projectId });
}

export function sha256Hex(s) {
  return crypto.createHash("sha256").update(String(s)).digest("hex");
}

export function truncateForBigQueryString(s, maxChars = 1_900_000) {
  if (s == null) return null;
  const str = String(s);
  if (str.length <= maxChars) return str;
  return str.slice(0, maxChars);
}

function fieldKey(f) {
  return String(f.name || "").toLowerCase();
}

function assertCompatibleField(existing, desired, tableId) {
  const exType = String(existing.type || "").toUpperCase();
  const deType = String(desired.type || "").toUpperCase();
  const exMode = String(existing.mode || "NULLABLE").toUpperCase();
  const deMode = String(desired.mode || "NULLABLE").toUpperCase();

  if (exType !== deType) {
    throw new Error(`Schema mismatch in ${tableId}: field ${existing.name} type ${exType} != ${deType}`);
  }

  // BigQuery allows relaxing REQUIRED -> NULLABLE, but not the reverse.
  if (exMode === "NULLABLE" && deMode === "REQUIRED") {
    throw new Error(`Schema mismatch in ${tableId}: field ${existing.name} mode NULLABLE -> REQUIRED not allowed`);
  }
}

export async function ensureTable(bq, datasetId, tableId, { schema, timePartitioning, clustering }) {
  const dataset = bq.dataset(datasetId);
  const table = dataset.table(tableId);
  const [exists] = await table.exists();

  if (!exists) {
    const options = { schema };
    if (timePartitioning) options.timePartitioning = timePartitioning;
    if (clustering) options.clustering = clustering;
    const [created] = await dataset.createTable(tableId, options);
    return created;
  }

  // If table already exists, add any missing fields (non-destructive schema evolution).
  if (Array.isArray(schema) && schema.length) {
    const [meta] = await table.getMetadata();
    const existingFields = Array.isArray(meta?.schema?.fields) ? meta.schema.fields : [];

    const existingByName = new Map(existingFields.map((f) => [fieldKey(f), f]));
    const merged = [...existingFields];

    let changed = false;
    for (const desiredField of schema) {
      const key = fieldKey(desiredField);
      const existingField = existingByName.get(key);
      if (!existingField) {
        merged.push(desiredField);
        changed = true;
        continue;
      }
      assertCompatibleField(existingField, desiredField, tableId);
    }

    if (changed) {
      await table.setMetadata({ schema: { fields: merged } });
    }
  }

  return table;
}

export async function insertRows(bqTable, rows) {
  if (!rows.length) return;
  await bqTable.insert(rows, { ignoreUnknownValues: true, skipInvalidRows: false });
}
