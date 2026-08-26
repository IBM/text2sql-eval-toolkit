import React, { useCallback, useEffect, useState } from "react";
import { Button } from "@carbon/react";
import { useLocation } from "react-router-dom";

import { parseLocation } from "../lib/routes";
import { shortenUrl, usePipelineAliases } from "../lib/pipelineAlias";

/**
 * Copies a shortened link to the current view.
 *
 * There is deliberately no "copy this address" control: the address is already
 * in the address bar, and a button that duplicates it earns nothing. This one
 * exists because what it produces *cannot* be obtained from the address bar --
 * pipeline ids are long, the comparison views carry two of them, and the result
 * gets wrapped by mail clients and truncated by chat apps. The alias form of the
 * same link reopens the identical view.
 *
 * So it appears only where it changes something: on an address that names a
 * pipeline.
 */

/** Copy text, falling back for browsers that withhold the clipboard API. */
async function copyText(text: string): Promise<void> {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }
  // Unavailable over plain http on some browsers, hence the fallback.
  const field = document.createElement("textarea");
  field.value = text;
  field.setAttribute("readonly", "");
  field.style.position = "fixed";
  field.style.opacity = "0";
  document.body.appendChild(field);
  field.select();
  // The return value is the only signal this path gives: a rejected copy is
  // silent otherwise, and the button would claim success having copied nothing.
  const copied = document.execCommand("copy");
  document.body.removeChild(field);
  if (!copied) throw new Error("copy was rejected by the browser");
}

/** True when this address carries a pipeline reference worth shortening. */
function hasPipelineRef(pathname: string, search: string): boolean {
  if (pathname.split("/").includes("pipeline")) return true;
  const params = new URLSearchParams(search.replace(/^\?/, ""));
  return Boolean(params.get("pipeline") || params.get("pipeline2"));
}

export const CopyShortLinkButton: React.FC = () => {
  const location = useLocation();
  const [state, setState] = useState<"idle" | "copied" | "failed">("idle");

  const shortenable = hasPipelineRef(location.pathname, location.search);
  const benchmarkId = parseLocation(location.pathname).benchmarkId;
  // Fetched only where the control is actually offered, so an ordinary page
  // view costs no extra request.
  const { table, ready } = usePipelineAliases(benchmarkId, shortenable);

  useEffect(() => {
    if (state === "idle") return;
    const timer = window.setTimeout(() => setState("idle"), 1800);
    return () => window.clearTimeout(timer);
  }, [state]);

  const copy = useCallback(async () => {
    const url = window.location.origin + location.pathname + location.search;
    try {
      await copyText(shortenUrl(url, table));
      setState("copied");
    } catch {
      setState("failed");
    }
  }, [location.pathname, location.search, table]);

  if (!shortenable) return null;

  return (
    <Button
      kind="ghost"
      size="sm"
      // Disabled until the alias table arrives. Clicking sooner shortened
      // against an empty table, which silently copies the original long URL
      // and still reports success -- the outcome this button exists to prevent.
      disabled={!ready}
      onClick={() => void copy()}
      title="Copy a shorter link to this view, using pipeline aliases"
      aria-live="polite"
    >
      {state === "copied"
        ? "Short link copied"
        : state === "failed"
          ? "Copy failed"
          : ready
            ? "Copy short link"
            : "Preparing short link…"}
    </Button>
  );
};
