import React, { useCallback, useEffect, useRef, useState } from "react";
import { Button, InlineLoading, InlineNotification } from "@carbon/react";
import { apiFetch } from "../lib/api";

interface ResultsStatus {
  fetch_enabled: boolean;
  has_results: boolean;
  results_path: string;
}

interface FetchJob {
  job_id: string;
  state: "queued" | "running" | "completed" | "failed";
  bytes_downloaded: number;
  total_bytes: number;
  error: string | null;
}

/**
 * Displays an informational banner when results are missing.
 * When the backend reports `fetch_enabled=true`, also shows a button that
 * triggers a background download from the Hugging Face Hub and polls for
 * progress every 2 seconds.
 */
export const FetchResultsBanner: React.FC<{ onResultsFetched?: () => void }> = ({
  onResultsFetched,
}) => {
  const [status, setStatus] = useState<ResultsStatus | null>(null);
  const [job, setJob] = useState<FetchJob | null>(null);
  const [dismissed, setDismissed] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const stopPolling = useCallback(() => {
    if (pollRef.current !== null) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  }, []);

  // Check results status on mount.
  useEffect(() => {
    apiFetch("/api/results/status")
      .then((r) => r.json() as Promise<ResultsStatus>)
      .then(setStatus)
      .catch(() => {
        // Silently ignore — the banner is best-effort.
      });
    return stopPolling;
  }, [stopPolling]);

  const pollJob = useCallback(
    (jobId: string) => {
      pollRef.current = setInterval(async () => {
        try {
          const r = await apiFetch(`/api/results/fetch/${jobId}`);
          const updated = (await r.json()) as FetchJob;
          setJob(updated);
          if (updated.state === "completed" || updated.state === "failed") {
            stopPolling();
            if (updated.state === "completed") {
              setStatus((prev) =>
                prev ? { ...prev, has_results: true } : prev
              );
              onResultsFetched?.();
            }
          }
        } catch {
          stopPolling();
        }
      }, 2000);
    },
    [stopPolling, onResultsFetched]
  );

  const handleFetch = async () => {
    try {
      const r = await apiFetch("/api/results/fetch", { method: "POST" });
      const newJob = (await r.json()) as FetchJob;
      setJob(newJob);
      pollJob(newJob.job_id);
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Failed to start fetch";
      setJob({
        job_id: "",
        state: "failed",
        bytes_downloaded: 0,
        total_bytes: 0,
        error: msg,
      });
    }
  };

  // Don't render if status not yet loaded, or results exist, or dismissed.
  if (!status || status.has_results || dismissed) return null;

  const isRunning = job && (job.state === "queued" || job.state === "running");
  const isFailed = job && job.state === "failed";

  if (isFailed) {
    return (
      <InlineNotification
        kind="error"
        title="Fetch failed"
        subtitle={job.error ?? "Unknown error"}
        lowContrast
        onCloseButtonClick={() => setDismissed(true)}
      />
    );
  }

  if (job?.state === "completed") {
    return (
      <InlineNotification
        kind="success"
        title="Results downloaded"
        subtitle="Reload the page to see the full dashboard."
        lowContrast
        onCloseButtonClick={() => setDismissed(true)}
      />
    );
  }

  return (
    <InlineNotification
      kind="info"
      title="No results found"
      subtitle={
        status.fetch_enabled ? (
          <span>
            Results directory is empty ({status.results_path}). Download
            pre-computed results from the Hugging Face Hub:
            {isRunning ? (
              <InlineLoading
                description="Downloading results…"
                style={{ display: "inline-flex", marginLeft: "0.5rem" }}
              />
            ) : (
              <Button
                kind="ghost"
                size="sm"
                style={{ marginLeft: "0.5rem", verticalAlign: "middle" }}
                onClick={() => void handleFetch()}
              >
                Fetch results
              </Button>
            )}
          </span>
        ) : (
          <span>
            Results directory is empty ({status.results_path}). Run:{" "}
            <code>text2sql-eval-toolkit results fetch</code>
          </span>
        )
      }
      lowContrast
      onCloseButtonClick={() => setDismissed(true)}
    />
  );
};
