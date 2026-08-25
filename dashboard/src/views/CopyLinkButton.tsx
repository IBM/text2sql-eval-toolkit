import React, { useCallback, useEffect, useState } from "react";
import { Button } from "@carbon/react";
import { useLocation } from "react-router-dom";

/**
 * Copies the current view's absolute URL.
 *
 * Every view is addressable, but that is only useful if the address is easy to
 * get hold of, so this lives in the header rather than being repeated per view.
 */
export const CopyLinkButton: React.FC = () => {
  const location = useLocation();
  const [copied, setCopied] = useState(false);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    if (!copied && !failed) return;
    const timer = window.setTimeout(() => {
      setCopied(false);
      setFailed(false);
    }, 1800);
    return () => window.clearTimeout(timer);
  }, [copied, failed]);

  const copy = useCallback(async () => {
    const url = window.location.origin + location.pathname + location.search;
    try {
      // Unavailable over plain http on some browsers, hence the fallback.
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(url);
      } else {
        const field = document.createElement("textarea");
        field.value = url;
        field.setAttribute("readonly", "");
        field.style.position = "fixed";
        field.style.opacity = "0";
        document.body.appendChild(field);
        field.select();
        document.execCommand("copy");
        document.body.removeChild(field);
      }
      setCopied(true);
    } catch {
      setFailed(true);
    }
  }, [location.pathname, location.search]);

  return (
    <Button
      kind="ghost"
      size="sm"
      onClick={() => void copy()}
      title="Copy a link to this view"
      aria-live="polite"
    >
      {copied ? "Link copied" : failed ? "Copy failed" : "Copy link"}
    </Button>
  );
};
