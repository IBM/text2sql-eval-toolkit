import { apiFetch, apiUrl } from "./api";

/**
 * Who the caller is and what this deployment serves.
 *
 * The UI uses this to avoid offering actions that would 403, and to say plainly
 * what a visitor is looking at: pre-computed results from a specific snapshot,
 * not a live evaluation.
 */

export interface JudgeUsage {
  month: string;
  spent_usd: number;
  budget_usd: number;
  remaining_usd: number;
  calls: number;
  warning: boolean;
}

export interface SessionInfo {
  tier: "public" | "judge" | "full";
  mode: "public" | "judge" | "full";
  email: string | null;
  signed_in: boolean;
  can_run_judge: boolean;
  can_mutate: boolean;
  /** Admin is a separate gate from the tier, so it has its own field. */
  role?: string;
  can_manage_users?: boolean;
  judge_usage?: JudgeUsage | null;
}

export interface DeploymentInfo {
  mode: string;
  toolkit_version: string;
  data_revision: string | null;
  data_provisioned_at: string | null;
  results_are_precomputed: boolean;
  sign_in_available: boolean;
  judge_available: boolean;
}

export async function fetchSession(): Promise<SessionInfo> {
  const res = await apiFetch(apiUrl("/api/me"));
  return res.json();
}

export async function fetchDeployment(): Promise<DeploymentInfo> {
  const res = await apiFetch(apiUrl("/api/deployment"));
  return res.json();
}

export async function signOut(): Promise<void> {
  await apiFetch(apiUrl("/api/auth/logout"), { method: "POST" });
}

/** Path that starts sign-in and returns to wherever the user was. */
export function signInHref(returnTo: string): string {
  return apiUrl(`/api/auth/login?next=${encodeURIComponent(returnTo)}`);
}

/**
 * Human-readable "data as of". Falls back to the raw value rather than showing
 * nothing, since an unparseable stamp is still information.
 */
export function formatSnapshot(
  deployment: DeploymentInfo | null,
): string | null {
  if (!deployment) return null;
  const { data_revision, data_provisioned_at } = deployment;
  if (!data_revision && !data_provisioned_at) return null;

  let when = "";
  if (data_provisioned_at) {
    const parsed = new Date(data_provisioned_at);
    when = Number.isNaN(parsed.getTime())
      ? data_provisioned_at
      : parsed.toISOString().slice(0, 10);
  }
  if (data_revision && when) return `${data_revision} · loaded ${when}`;
  return data_revision ?? when;
}

/** Whether this deployment can offer anything beyond browsing. */
export function isReadOnly(session: SessionInfo | null): boolean {
  if (!session) return false;
  return !session.can_mutate && !session.can_run_judge;
}
