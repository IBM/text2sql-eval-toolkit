/**
 * Which pipeline's detail to show for a record.
 *
 * Two paths reach this: clicking a row, and restoring a record from a shared
 * URL. They must agree, or a shared link opens the detail panel on a different
 * pipeline than the sender saw — or, if the restore path forgets to resolve a
 * pipeline at all, on an empty panel.
 */
export interface RecordWithPredictions {
  record_id: string;
  predictions: Record<string, unknown>;
}

export function resolveDetailPipeline(
  recordId: string,
  records: RecordWithPredictions[],
  preferredPipeline?: string | null
): string | null {
  const record = records.find((r) => r.record_id === recordId);
  const available = Object.keys(record?.predictions ?? {});
  if (preferredPipeline && available.includes(preferredPipeline)) {
    return preferredPipeline;
  }
  return available[0] ?? null;
}
