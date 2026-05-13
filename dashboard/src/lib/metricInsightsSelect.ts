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
