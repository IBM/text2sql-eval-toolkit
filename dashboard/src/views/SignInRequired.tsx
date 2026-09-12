import React, { useEffect, useState } from "react";
import { Button, InlineNotification } from "@carbon/react";
import { useLocation, useNavigate } from "react-router-dom";

import {
  type DeploymentInfo,
  fetchDeployment,
  signInHref,
} from "../lib/session";

/**
 * What a link into a benchmark's details shows a reader who is not signed in.
 *
 * Some benchmarks publish only their overall scores; their questions, SQL and
 * per-record results need sign-in, and the server refuses every route that
 * would show them. Without this the reader would see a view full of failed
 * requests -- or, for a benchmark not in their listing, be told the server has
 * no such benchmark -- and a link a colleague shared would look dead rather
 * than one sign-in away. Signing in returns to the same address.
 *
 * `summaryHref`, when given, is where the public part of the benchmark is.
 */
export const SignInRequired: React.FC<{
  benchmarkId: string;
  summaryHref?: string;
}> = ({ benchmarkId, summaryHref }) => {
  const location = useLocation();
  const navigate = useNavigate();
  const [deployment, setDeployment] = useState<DeploymentInfo | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetchDeployment()
      .then((d) => {
        if (!cancelled) setDeployment(d);
      })
      .catch(() => {
        /* Without it there is no sign-in to offer; the message still stands. */
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const returnTo = `${location.pathname}${location.search}` || "/";
  const canSignIn = !!deployment?.sign_in_available;
  const what = summaryHref
    ? `"${benchmarkId}" publishes its overall scores; its questions, SQL and per-record results are only available to signed-in users`
    : `"${benchmarkId}" is only available to signed-in users`;

  return (
    <div style={{ maxWidth: "760px", margin: "0 auto", padding: "1rem" }}>
      <InlineNotification
        kind="info"
        title="Sign in to view this"
        subtitle={
          canSignIn
            ? `${what}.`
            : `${what}, and this server does not offer sign-in.`
        }
        lowContrast
        hideCloseButton
      />
      <div style={{ display: "flex", gap: "0.5rem" }}>
        {canSignIn && (
          // A plain link, not fetch(): sign-in is a top-level redirect to
          // Google and back.
          <Button kind="primary" size="sm" href={signInHref(returnTo)} as="a">
            Sign in
          </Button>
        )}
        {summaryHref && (
          <Button
            kind="tertiary"
            size="sm"
            onClick={() => navigate(summaryHref)}
          >
            See overall scores
          </Button>
        )}
      </div>
    </div>
  );
};
