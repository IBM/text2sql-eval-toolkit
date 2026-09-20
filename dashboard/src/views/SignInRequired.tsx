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
  // Three states, not two: "we have not asked yet" is not "the server has no
  // sign-in". Collapsing them told every reader, for as long as the request
  // took, that there was no way in -- and left them there for good if the
  // request failed.
  const [status, setStatus] = useState<"asking" | "answered" | "failed">(
    "asking",
  );

  useEffect(() => {
    let cancelled = false;
    fetchDeployment()
      .then((d) => {
        if (cancelled) return;
        setDeployment(d);
        setStatus("answered");
      })
      .catch(() => {
        if (cancelled) return;
        // Offer sign-in anyway: a deployment that has it is far likelier than
        // one that does not, and a link that cannot be followed is the worse
        // of the two wrong answers.
        setStatus("failed");
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const returnTo = `${location.pathname}${location.search}` || "/";
  const canSignIn =
    status === "failed" ? true : !!deployment?.sign_in_available;
  const knownUnavailable =
    status === "answered" && !deployment?.sign_in_available;
  const what = summaryHref
    ? `"${benchmarkId}" publishes its overall scores; its questions, SQL and per-record results are only available to signed-in users`
    : `"${benchmarkId}" is only available to signed-in users`;

  return (
    <div style={{ maxWidth: "760px", margin: "0 auto", padding: "1rem" }}>
      <InlineNotification
        kind="info"
        title="Sign in to view this"
        subtitle={
          knownUnavailable
            ? `${what}, and this server does not offer sign-in.`
            : `${what}.`
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
