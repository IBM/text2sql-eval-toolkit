import React from "react";
import { Button } from "@carbon/react";
import { Launch } from "@carbon/icons-react";
import { routes } from "../lib/routes";

/**
 * Take the record on screen into the Eval Playground.
 *
 * The detail panels show a record read-only: the question, the two queries,
 * the results, the metrics. The playground is where the same record can be
 * edited and re-run. Getting between them meant reading the record id off the
 * address bar and assembling a `/run/...` URL by hand, which is a thing nobody
 * does twice.
 *
 * A real anchor, not a button with a click handler. The playground address is
 * worth copying and worth opening in a new tab, and only a plain left click is
 * intercepted for single-page navigation -- every other gesture is handed back
 * to the browser, which already knows what modifier-click means.
 *
 * Without `onNavigate` it stays a link and the browser navigates normally, so
 * the control degrades rather than breaking if a caller forgets to wire it.
 */

interface Props {
  benchmarkId: string | null | undefined;
  recordId: string | null | undefined;
  /**
   * Opened against this pipeline. The playground shows one prediction at a
   * time, so without it the view picks its own default and the reader lands on
   * a different pipeline than the one they were just looking at.
   */
  pipeline?: string | null;
  onNavigate?: (href: string) => void;
  size?: "sm" | "md" | "lg";
}

export const OpenInPlaygroundButton: React.FC<Props> = ({
  benchmarkId,
  recordId,
  pipeline,
  onNavigate,
  size = "sm",
}) => {
  if (!benchmarkId || !recordId) return null;

  const href = routes.run(benchmarkId, recordId, pipeline ?? null);

  const handleClick = (event: React.MouseEvent<HTMLElement>) => {
    if (!onNavigate) return;
    if (
      event.defaultPrevented ||
      event.button !== 0 ||
      event.metaKey ||
      event.ctrlKey ||
      event.shiftKey ||
      event.altKey
    ) {
      return;
    }
    event.preventDefault();
    onNavigate(href);
  };

  return (
    <Button
      as="a"
      href={href}
      kind="ghost"
      size={size}
      renderIcon={Launch}
      // Carbon types Button's handler as a button event even when it renders
      // an anchor; the event itself is the anchor's.
      onClick={handleClick as React.MouseEventHandler<HTMLButtonElement>}
    >
      Open in Eval Playground
    </Button>
  );
};
