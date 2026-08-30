import React, { useState } from "react";
import { OverflowMenu, OverflowMenuItem } from "@carbon/react";

import {
  type ExportableRecord,
  exportFilename,
  toHtml,
  toMarkdown,
} from "../lib/playgroundExport";

/**
 * Export one playground record.
 *
 * This replaced "Copy short link" here. That control exists to shorten an
 * address carrying two long pipeline ids, which is a comparison-view problem;
 * on the playground the address is already short, and what people actually want
 * to take away is the record itself.
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
  // some browsers before it has read the blob.
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

  const label =
    state === "copied"
      ? "URL copied"
      : state === "failed"
        ? "Copy failed"
        : "Export";

  const copyUrl = async () => {
    if (!record) return;
    try {
      await copyText(record.url);
      setState("copied");
      window.setTimeout(() => setState("idle"), 1800);
    } catch {
      setState("failed");
      window.setTimeout(() => setState("idle"), 1800);
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

  const printPdf = () => {
    if (!record) return;
    // The browser's own print-to-PDF, rather than a ~300 KB PDF writer bundled
    // to reproduce it. Opened in a new window so printing does not take the
    // dashboard's own layout with it.
    const win = window.open("", "_blank", "noopener,width=900,height=700");
    if (!win) {
      setState("failed");
      window.setTimeout(() => setState("idle"), 2500);
      return;
    }
    win.document.write(toHtml(record));
    win.document.close();
    win.focus();
    // After load, or the window may print an empty document.
    win.onload = () => win.print();
  };

  return (
    <OverflowMenu
      renderIcon={() => <span style={{ fontSize: "0.8125rem" }}>{label}</span>}
      aria-label="Export this record"
      flipped
      disabled={!record}
      size="sm"
      // Carbon sizes the menu to its trigger, and "Copy URL to clipboard" is
      // wider than "Export" -- so without this the first item truncates to
      // "Copy URL to clipb...".
      menuOptionsClass="playground-export-menu"
    >
      <OverflowMenuItem
        itemText="Copy URL to clipboard"
        onClick={() => void copyUrl()}
      />
      <OverflowMenuItem itemText="PDF (via print)" onClick={printPdf} />
      <OverflowMenuItem itemText="Markdown (.md)" onClick={saveMarkdown} />
      <OverflowMenuItem itemText="HTML (.html)" onClick={saveHtml} />
    </OverflowMenu>
  );
};
