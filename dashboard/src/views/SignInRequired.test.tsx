import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { SignInRequired } from "./SignInRequired";
import type { DeploymentInfo } from "../lib/session";

/**
 * A link into a benchmark's details, opened by someone not signed in.
 *
 * The failure this guards against is the link reading as dead: "not found", or
 * a sign-in that drops the reader on the home page instead of where the link
 * pointed -- and, for a benchmark whose overall scores are public, no way to
 * reach those scores.
 */

const deployment = (over: Partial<DeploymentInfo> = {}): DeploymentInfo => ({
  mode: "public",
  toolkit_version: "1.6.0",
  data_revision: "v1.6.0",
  data_provisioned_at: null,
  results_are_precomputed: true,
  sign_in_available: true,
  judge_available: false,
  ...over,
});

function stubDeployment(d: DeploymentInfo) {
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(JSON.stringify(d), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
    ),
  );
}

const at = (url: string, summaryHref?: string) =>
  render(
    <MemoryRouter initialEntries={[url]}>
      <Routes>
        <Route
          path="/benchmark/:id"
          element={<div>summary page</div>}
        />
        <Route
          path="*"
          element={
            <SignInRequired benchmarkId="beaver" summaryHref={summaryHref} />
          }
        />
      </Routes>
    </MemoryRouter>,
  );

beforeEach(() => {
  vi.unstubAllGlobals();
});

describe("SignInRequired", () => {
  it("offers sign-in that returns to the same address", async () => {
    stubDeployment(deployment());
    at("/errors?benchmark=beaver&page=2");

    const link = await screen.findByRole("link", { name: "Sign in" });
    expect(link.getAttribute("href")).toBe(
      `/api/auth/login?next=${encodeURIComponent("/errors?benchmark=beaver&page=2")}`,
    );
    expect(screen.getByText("Sign in to view this")).toBeTruthy();
    expect(screen.queryByText(/not found/i)).toBeNull();
  });

  it("says so plainly where sign-in is not configured", async () => {
    stubDeployment(deployment({ sign_in_available: false }));
    at("/run/beaver");

    await waitFor(() =>
      expect(
        screen.getByText(/this server does not offer sign-in/),
      ).toBeTruthy(),
    );
    expect(screen.queryByRole("link", { name: "Sign in" })).toBeNull();
  });

  it("says nothing about sign-in while it is still asking", async () => {
    // A request that never settles: the reader must not be told there is no
    // way in before the server has said so.
    vi.stubGlobal(
      "fetch",
      vi.fn(() => new Promise<Response>(() => {})),
    );
    at("/run/beaver");

    expect(screen.getByText("Sign in to view this")).toBeTruthy();
    expect(screen.queryByText(/does not offer sign-in/)).toBeNull();
  });

  it("still offers sign-in when the deployment cannot be read", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        throw new Error("offline");
      }),
    );
    at("/errors?benchmark=beaver");

    expect(await screen.findByRole("link", { name: "Sign in" })).toBeTruthy();
    expect(screen.queryByText(/does not offer sign-in/)).toBeNull();
  });

  it("points at the public overall scores when there are some", async () => {
    stubDeployment(deployment());
    at("/errors?benchmark=beaver", "/benchmark/beaver");

    expect(
      await screen.findByText(/publishes its overall scores/),
    ).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "See overall scores" }));
    expect(await screen.findByText("summary page")).toBeTruthy();
  });
});
