import React, { useCallback, useEffect, useState } from "react";
import { Button, Tag } from "@carbon/react";
import { useLocation } from "react-router-dom";

import {
  type DeploymentInfo,
  type SessionInfo,
  fetchDeployment,
  fetchSession,
  formatSnapshot,
  signInHref,
  signOut,
} from "../lib/session";

/**
 * Header strip: who you are, what this deployment allows, and how current the
 * data is.
 *
 * A shared link may be opened months later by someone who has never seen the
 * tool, so the two things worth saying without being asked are that the results
 * are pre-computed from a specific snapshot, and that browsing is read-only.
 */
export const SessionBar: React.FC = () => {
  const location = useLocation();
  const [session, setSession] = useState<SessionInfo | null>(null);
  const [deployment, setDeployment] = useState<DeploymentInfo | null>(null);

  const load = useCallback(async () => {
    // Never let this strip break the page it decorates.
    const [s, d] = await Promise.allSettled([fetchSession(), fetchDeployment()]);
    if (s.status === "fulfilled") setSession(s.value);
    if (d.status === "fulfilled") setDeployment(d.value);
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const onSignOut = useCallback(async () => {
    try {
      await signOut();
    } finally {
      await load();
    }
  }, [load]);

  const returnTo = `${location.pathname}${location.search}` || "/";

  // Local mode is the operator's own tool; none of this is worth the space.
  if (session?.mode === "full") {
    return null;
  }

  return (
    // Single row that never wraps: the header has little spare width, and a
    // wrapped strip collides with the title. The data stamp lives in its own
    // strip below (DataStampBar), which has room for it.
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: "0.5rem",
        flexWrap: "nowrap",
        justifyContent: "flex-end",
        whiteSpace: "nowrap",
      }}
    >
      {session && !session.can_mutate && (
        <Tag type="cool-gray" size="sm" title="Browsing only on this deployment">
          Read-only
        </Tag>
      )}

      {session?.can_run_judge && (
        <Tag type="green" size="sm" title="You can run LLM-as-judge on a record">
          Judge enabled
        </Tag>
      )}

      {session?.signed_in ? (
        <>
          <span style={{ fontSize: "0.75rem", opacity: 0.8 }}>{session.email}</span>
          <Button kind="ghost" size="sm" onClick={() => void onSignOut()}>
            Sign out
          </Button>
        </>
      ) : (
        deployment?.sign_in_available && (
          // A plain link, not fetch(): sign-in is a top-level redirect to
          // Google and back.
          <Button kind="ghost" size="sm" href={signInHref(returnTo)} as="a">
            Sign in
          </Button>
        )
      )}
    </div>
  );
};


/**
 * Thin strip under the header naming the snapshot on screen.
 *
 * Deliberately on every page rather than just the landing page: shared links
 * open deep, and "which data is this?" is exactly the question a recipient has.
 */
export const DataStampBar: React.FC = () => {
  const [deployment, setDeployment] = useState<DeploymentInfo | null>(null);

  useEffect(() => {
    fetchDeployment()
      .then(setDeployment)
      .catch(() => setDeployment(null));
  }, []);

  if (!deployment || deployment.mode === "full") return null;
  const snapshot = formatSnapshot(deployment);
  if (!snapshot) return null;

  return (
    <div
      style={{
        padding: "0.3rem 1.25rem",
        fontSize: "0.75rem",
        opacity: 0.7,
        borderBottom: "1px solid rgba(255,255,255,0.08)",
        background: "rgba(255,255,255,0.02)",
      }}
    >
      Showing pre-computed results from snapshot <strong>{snapshot}</strong> —
      this page does not run evaluations live.
    </div>
  );
};
