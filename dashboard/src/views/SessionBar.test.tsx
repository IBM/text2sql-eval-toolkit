import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { DataStampBar, SessionBar } from "./SessionBar";
import type { DeploymentInfo, SessionInfo } from "../lib/session";

/**
 * The header strip decides what a visitor is offered.
 *
 * Getting it wrong is not cosmetic: showing a sign-in button on a server with no
 * OAuth configured, or omitting the read-only marker on a deployment that cannot
 * mutate anything, both mislead about what the page can do.
 */

const session = (over: Partial<SessionInfo> = {}): SessionInfo => ({
  tier: "public",
  mode: "public",
  email: null,
  signed_in: false,
  can_run_judge: false,
  can_mutate: false,
  ...over,
});

const deployment = (over: Partial<DeploymentInfo> = {}): DeploymentInfo => ({
  mode: "public",
  toolkit_version: "1.1.0",
  data_revision: "v1.1.0",
  data_provisioned_at: "2026-08-25T16:15:14Z",
  results_are_precomputed: true,
  sign_in_available: false,
  judge_available: false,
  ...over,
});

function stubApi(s: SessionInfo, d: DeploymentInfo) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      const body = url.includes("/api/deployment") ? d : s;
      return new Response(JSON.stringify(body), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    })
  );
}

const inRouter = (ui: React.ReactElement) =>
  render(<MemoryRouter initialEntries={["/b/demo/errors?page=2"]}>{ui}</MemoryRouter>);

beforeEach(() => {
  vi.unstubAllGlobals();
});

describe("SessionBar", () => {
  it("marks a deployment that cannot mutate as read-only", async () => {
    stubApi(session(), deployment());
    inRouter(<SessionBar />);
    expect(await screen.findByText("Read-only")).toBeInTheDocument();
  });

  it("offers sign-in only when the server has OAuth configured", async () => {
    stubApi(session(), deployment({ sign_in_available: false }));
    inRouter(<SessionBar />);
    await screen.findByText("Read-only");
    expect(screen.queryByText("Sign in")).not.toBeInTheDocument();
  });

  it("offers sign-in when the server does have OAuth configured", async () => {
    stubApi(session(), deployment({ sign_in_available: true }));
    inRouter(<SessionBar />);
    expect(await screen.findByText("Sign in")).toBeInTheDocument();
  });

  it("returns the user to where they were after signing in", async () => {
    stubApi(session(), deployment({ sign_in_available: true }));
    inRouter(<SessionBar />);
    const link = await screen.findByText("Sign in");
    const href = link.closest("a")?.getAttribute("href") ?? "";
    expect(href).toContain(encodeURIComponent("/b/demo/errors?page=2"));
  });

  it("shows the signed-in address and a way out", async () => {
    stubApi(
      session({ signed_in: true, email: "someone@example.com", tier: "judge", can_run_judge: true }),
      deployment({ sign_in_available: true, judge_available: true })
    );
    inRouter(<SessionBar />);
    expect(await screen.findByText("someone@example.com")).toBeInTheDocument();
    expect(screen.getByText("Sign out")).toBeInTheDocument();
    expect(screen.queryByText("Sign in")).not.toBeInTheDocument();
  });

  it("marks the judge tier only when it is actually available", async () => {
    stubApi(session({ can_run_judge: true, tier: "judge" }), deployment());
    inRouter(<SessionBar />);
    expect(await screen.findByText("Judge enabled")).toBeInTheDocument();
  });

  it("renders nothing at all for a local operator", async () => {
    stubApi(session({ mode: "full", tier: "full", can_mutate: true }), deployment({ mode: "full" }));
    const { container } = inRouter(<SessionBar />);
    await waitFor(() => expect(container.textContent).toBe(""));
  });

  it("survives the API being unreachable rather than breaking the header", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => { throw new Error("network down"); }));
    const { container } = inRouter(<SessionBar />);
    await waitFor(() => expect(container).toBeTruthy());
    expect(screen.queryByText("Read-only")).not.toBeInTheDocument();
  });
});

describe("DataStampBar", () => {
  it("names the snapshot and says results are not live", async () => {
    stubApi(session(), deployment());
    inRouter(<DataStampBar />);
    const strip = await screen.findByText(/pre-computed results/i);
    expect(strip.textContent).toContain("v1.1.0");
    expect(strip.textContent).toContain("2026-08-25");
    expect(strip.textContent).toMatch(/does not run evaluations live/i);
  });

  it("stays out of the way on a local run", async () => {
    stubApi(session({ mode: "full" }), deployment({ mode: "full" }));
    const { container } = inRouter(<DataStampBar />);
    await waitFor(() => expect(container.textContent).toBe(""));
  });

  it("renders nothing rather than a blank stamp when the snapshot is unknown", async () => {
    stubApi(session(), deployment({ data_revision: null, data_provisioned_at: null }));
    const { container } = inRouter(<DataStampBar />);
    await waitFor(() => expect(container.textContent).toBe(""));
  });
});
