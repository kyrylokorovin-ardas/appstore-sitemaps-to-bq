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

export async function ensureTable(bq, datasetId, tableId, { schema, timePartitioning, clustering }) {
  const dataset = bq.dataset(datasetId);
  const table = dataset.table(tableId);
  const [exists] = await table.exists();
  if (exists) return table;

  const options = { schema };
  if (timePartitioning) options.timePartitioning = timePartitioning;
  if (clustering) options.clustering = clustering;
  const [created] = await dataset.createTable(tableId, options);
  return created;
}

export async function insertRows(bqTable, rows) {
  if (!rows.length) return;
  await bqTable.insert(rows, { ignoreUnknownValues: true, skipInvalidRows: false });
}
