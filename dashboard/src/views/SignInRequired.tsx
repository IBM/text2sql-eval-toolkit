import React, { useEffect, useState } from "react";
import { Button, InlineNotification } from "@carbon/react";
import { useLocation } from "react-router-dom";

import {
  type DeploymentInfo,
  fetchDeployment,
  signInHref,
} from "../lib/session";

/**
 * What a link to a sign-in-only benchmark shows a reader who is not signed in.
 *
 * The server leaves such a benchmark out of an anonymous listing and refuses
 * every route that names it, so without this the reader would be told the
 * server has no such benchmark -- and a link a colleague shared would look
 * dead rather than one sign-in away. Signing in returns to the same address.
 */
export const SignInRequired: React.FC<{ benchmarkId: string }> = ({
  benchmarkId,
}) => {
  const location = useLocation();
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

  return (
    <div style={{ maxWidth: "760px", margin: "0 auto", padding: "1rem" }}>
      <InlineNotification
        kind="info"
        title="Sign in to view this benchmark"
        subtitle={
          canSignIn
            ? `"${benchmarkId}" is only available to signed-in users.`
            : `"${benchmarkId}" is only available to signed-in users, and this server does not offer sign-in.`
        }
        lowContrast
        hideCloseButton
      />
      {canSignIn && (
        // A plain link, not fetch(): sign-in is a top-level redirect to Google
        // and back.
        <Button kind="primary" size="sm" href={signInHref(returnTo)} as="a">
          Sign in
        </Button>
      )}
    </div>
  );
};
