import React, { useState } from "react";
import { OverflowMenu, OverflowMenuItem } from "@carbon/react";
import { CheckmarkFilled, Export, WarningFilled } from "@carbon/icons-react";

import {
  type ExportableRecord,
  exportFilename,
  toHtml,
  toMarkdown,
} from "../lib/playgroundExport";

/**
 * Export one playground record.
 *
 * This replaced "Copy short link" on this view. That control exists to shorten
 * an address carrying two long pipeline ids, which is a comparison-view problem;
 * the playground's address is already short, and what people want to take away
 * is the record.
 *
 * The playground is where a disagreement about a score gets settled, and those
 * arguments happen in issues, reviews and papers rather than in the tool -- so
 * the export carries the question, both statements, both result sets and every
 * metric, and links back to the address it came from.
 */

interface Props {
  /** Null while nothing is loaded, which disables the menu. */
  record: ExportableRecord | null;
}

function download(filename: string, contents: string, mime: string): void {
  const blob = new Blob([contents], { type: `${mime};charset=utf-8` });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  // Revoked on the next tick: revoking synchronously can cancel the download in
  // some browsers before they have read the blob.
  setTimeout(() => URL.revokeObjectURL(url), 0);
}

async function copyText(text: string): Promise<void> {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const field = document.createElement("textarea");
  field.value = text;
  field.setAttribute("readonly", "");
  field.style.position = "fixed";
  field.style.opacity = "0";
  document.body.appendChild(field);
  field.select();
  const copied = document.execCommand("copy");
  document.body.removeChild(field);
  if (!copied) throw new Error("copy was rejected by the browser");
}

export const ExportMenu: React.FC<Props> = ({ record }) => {
  const [state, setState] = useState<"idle" | "copied" | "failed">("idle");

  const flash = (next: "copied" | "failed") => {
    setState(next);
    window.setTimeout(() => setState("idle"), 1800);
  };

  const copyUrl = async () => {
    if (!record) return;
    try {
      await copyText(record.url);
      flash("copied");
    } catch {
      flash("failed");
    }
  };

  const saveMarkdown = () => {
    if (!record) return;
    download(exportFilename(record, "md"), toMarkdown(record), "text/markdown");
  };

  const saveHtml = () => {
    if (!record) return;
    download(exportFilename(record, "html"), toHtml(record), "text/html");
  };

  // The icon doubles as the outcome of the last action, because an overflow
  // menu closes on click and there is nowhere else to say whether the copy
  // worked.
  const Icon =
    state === "copied"
      ? CheckmarkFilled
      : state === "failed"
        ? WarningFilled
        : Export;
  const label =
    state === "copied"
      ? "URL copied to clipboard"
      : state === "failed"
        ? "Could not copy — copy it from the address bar"
        : "Export this record";

  return (
    <OverflowMenu
      renderIcon={Icon}
      iconDescription={label}
      aria-label={label}
      title={label}
      flipped
      disabled={!record}
      menuOptionsClass="playground-export-menu"
      size="sm"
    >
      <OverflowMenuItem
        itemText="Copy URL to clipboard"
        onClick={() => void copyUrl()}
      />
      <OverflowMenuItem itemText="Markdown (.md)" onClick={saveMarkdown} />
      <OverflowMenuItem itemText="HTML (.html)" onClick={saveHtml} />
    </OverflowMenu>
  );
};
