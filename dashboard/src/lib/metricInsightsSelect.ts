export interface MetricDefinition {
  group: string;
  name: string;
  description: string;
  value_type: string;
}

export interface MetricDefinitionsResponse {
  groups: string[];
  metrics: MetricDefinition[];
}

export interface MetricInsightsSelectGroup {
  label: string;
  metrics: { name: string; description: string }[];
}

/**
 * Group flat metric definitions for Carbon SelectItemGroup, preserving group
 * order by first appearance in `metrics` (same order as the toolkit payload).
 */
export function buildMetricInsightsSelectGroups(
  metrics: MetricDefinition[]
): MetricInsightsSelectGroup[] {
  const order: string[] = [];
  const byGroup = new Map<string, MetricDefinition[]>();

  for (const m of metrics) {
    if (!byGroup.has(m.group)) {
      order.push(m.group);
      byGroup.set(m.group, []);
    }
    byGroup.get(m.group)!.push(m);
  }

  return order.map((label) => ({
    label,
    metrics: (byGroup.get(label) ?? []).map((m) => ({
      name: m.name,
      description: m.description,
    })),
  }));
}

export function flattenMetricInsightsSelectNames(
  groups: MetricInsightsSelectGroup[]
): string[] {
  const names: string[] = [];
  for (const g of groups) {
    for (const m of g.metrics) {
      names.push(m.name);
    }
  }
  return names;
}

/**
 * Clamp a chosen metric to the ones the server actually defines.
 *
 * Several views let the user pick a metric and then need that choice to remain
 * valid when the available set changes. Doing it in an effect meant a render
 * with an invalid selection before the correction landed -- and, in one case, an
 * effect that depended on the value it also set. Deriving the effective value
 * removes both, and putting the rule here means the three views cannot drift.
 *
 * Returns the choice unchanged when it is still available, or when nothing is
 * available yet (so a slow definitions fetch does not reset the user's pick).
 */
export function clampToAvailable(
  chosen: string,
  available: string[],
  fallbackIndex = 0
): string {
  if (available.length === 0) return chosen;
  if (available.includes(chosen)) return chosen;
  return available[fallbackIndex] ?? available[0];
}
