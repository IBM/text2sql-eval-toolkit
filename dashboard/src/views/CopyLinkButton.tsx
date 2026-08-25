import React, { useCallback, useEffect, useState } from "react";
import { Button } from "@carbon/react";
import { useLocation } from "react-router-dom";

import { parseLocation } from "../lib/routes";
import { shortenUrl, usePipelineAliases } from "../lib/pipelineAlias";

/**
 * Copies the current view's absolute URL.
 *
 * Every view is addressable, but that is only useful if the address is easy to
 * get hold of, so this lives in the header rather than being repeated per view.
 *
 * A second control appears when the address names a pipeline. Pipeline ids are
 * long -- and the comparison views carry two of them -- so the readable link
 * gets wrapped by mail clients and truncated by chat apps. The short form uses
 * aliases and reopens the identical view. The readable form stays the default,
 * since it says what it points at.
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
  document.execCommand("copy");
  document.body.removeChild(field);
}

/** True when this address carries a pipeline reference worth shortening. */
function hasPipelineRef(pathname: string, search: string): boolean {
  if (pathname.split("/").includes("pipeline")) return true;
  const params = new URLSearchParams(search.replace(/^\?/, ""));
  return Boolean(params.get("pipeline") || params.get("pipeline2"));
}

export const CopyLinkButton: React.FC = () => {
  const location = useLocation();
  const [state, setState] = useState<"idle" | "long" | "short" | "failed">(
    "idle"
  );

  const shortenable = hasPipelineRef(location.pathname, location.search);
  const benchmarkId = parseLocation(location.pathname).benchmarkId;
  // Fetched only where a short link is actually offered, so an ordinary page
  // view costs no extra request.
  const { table } = usePipelineAliases(benchmarkId, shortenable);

  useEffect(() => {
    if (state === "idle") return;
    const timer = window.setTimeout(() => setState("idle"), 1800);
    return () => window.clearTimeout(timer);
  }, [state]);

  const copy = useCallback(
    async (short: boolean) => {
      const url = window.location.origin + location.pathname + location.search;
      try {
        await copyText(short ? shortenUrl(url, table) : url);
        setState(short ? "short" : "long");
      } catch {
        setState("failed");
      }
    },
    [location.pathname, location.search, table]
  );

  return (
    <>
      <Button
        kind="ghost"
        size="sm"
        onClick={() => void copy(false)}
        title="Copy a link to this view"
        aria-live="polite"
      >
        {state === "long"
          ? "Link copied"
          : state === "failed"
            ? "Copy failed"
            : "Copy link"}
      </Button>
      {shortenable && (
        <Button
          kind="ghost"
          size="sm"
          onClick={() => void copy(true)}
          title="Copy a shorter link to this view, using pipeline aliases"
          aria-live="polite"
        >
          {state === "short" ? "Short link copied" : "Copy short link"}
        </Button>
      )}
    </>
  );
};
