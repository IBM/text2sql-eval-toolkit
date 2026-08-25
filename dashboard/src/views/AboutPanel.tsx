import React, { useEffect, useState } from "react";
import { CodeSnippet, Link } from "@carbon/react";

import { type DeploymentInfo, fetchDeployment, formatSnapshot } from "../lib/session";

const REPO_URL = "https://github.com/IBM/text2sql-eval-toolkit";
const PYPI_URL = "https://pypi.org/project/text2sql-eval-toolkit/";
const RESULTS_URL =
  "https://huggingface.co/datasets/text2sql-eval-toolkit/text2sql-eval-results";

const CITATION = `@unpublished{hassanzadeh2026text2sql,
  title  = {{Text-to-SQL Evaluation Toolkit}},
  author = {Hassanzadeh, Oktie and Perlitz, Yotam and Pham, Nhan and
            Kaple, Tanvi and Zrobek, Karolina and Vu, Long and Glass, Michael and
            Subramanian, Dharmashankar and Pourreza, Mohammadreza and Rafiei, Davood},
  year   = {2026},
  note   = {Under submission},
}`;

/**
 * Context for someone arriving from a shared link.
 *
 * Such a visitor may never have seen the toolkit, so this says what the numbers
 * are, where they came from, and how to cite them -- without which a link is
 * just a table of figures with no provenance.
 */
export const AboutPanel: React.FC = () => {
  const [deployment, setDeployment] = useState<DeploymentInfo | null>(null);

  useEffect(() => {
    fetchDeployment()
      .then(setDeployment)
      .catch(() => setDeployment(null));
  }, []);

  // The operator's own machine needs none of this.
  if (deployment?.mode === "full") return null;

  const snapshot = formatSnapshot(deployment);

  return (
    <div
      style={{
        border: "1px solid rgba(255,255,255,0.12)",
        borderRadius: "8px",
        padding: "1rem 1.1rem",
        background: "rgba(255,255,255,0.015)",
        display: "flex",
        flexDirection: "column",
        gap: "0.6rem",
      }}
    >
      <h4 style={{ margin: 0, fontSize: "0.95rem" }}>About these results</h4>

      <p style={{ margin: 0, opacity: 0.9, lineHeight: 1.45, fontSize: "0.875rem" }}>
        Every figure here is <strong>pre-computed</strong>. Inference, SQL
        execution, and evaluation were run offline with the{" "}
        <Link href={REPO_URL} target="_blank" rel="noreferrer">
          text2sql-eval-toolkit
        </Link>
        ; this page reads the published results and does not evaluate anything
        live.
        {snapshot && (
          <>
            {" "}
            The data shown is snapshot <strong>{snapshot}</strong>, so a link
            shared from this page keeps showing the same numbers.
          </>
        )}
      </p>

      <p style={{ margin: 0, opacity: 0.9, lineHeight: 1.45, fontSize: "0.875rem" }}>
        Every view is addressable — use <strong>Copy link</strong> in the header
        to share the exact benchmark, pipeline, filter, or individual record you
        are looking at.
      </p>

      <div style={{ display: "flex", gap: "1rem", flexWrap: "wrap", fontSize: "0.8125rem" }}>
        <Link href={REPO_URL} target="_blank" rel="noreferrer">
          Source
        </Link>
        <Link href={PYPI_URL} target="_blank" rel="noreferrer">
          PyPI
        </Link>
        <Link href={RESULTS_URL} target="_blank" rel="noreferrer">
          Published results
        </Link>
        {deployment?.toolkit_version && (
          <span style={{ opacity: 0.6 }}>toolkit {deployment.toolkit_version}</span>
        )}
      </div>

      <details>
        <summary style={{ cursor: "pointer", fontSize: "0.8125rem", opacity: 0.85 }}>
          Cite this work
        </summary>
        <div style={{ marginTop: "0.5rem" }}>
          <CodeSnippet type="multi" feedback="Copied">
            {CITATION}
          </CodeSnippet>
        </div>
      </details>
    </div>
  );
};
