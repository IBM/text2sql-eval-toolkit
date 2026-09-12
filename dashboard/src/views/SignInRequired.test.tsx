import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { SignInRequired } from "./SignInRequired";
import type { DeploymentInfo } from "../lib/session";

/**
 * A shared link to a sign-in-only benchmark, opened by someone not signed in.
 *
 * The failure this guards against is the link reading as dead: "not found", or
 * a sign-in that drops the reader on the home page instead of where the link
 * pointed.
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

const at = (url: string) =>
  render(
    <MemoryRouter initialEntries={[url]}>
      <SignInRequired benchmarkId="beaver" />
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
    expect(screen.getByText("Sign in to view this benchmark")).toBeTruthy();
    expect(screen.queryByText(/not found/i)).toBeNull();
  });

  it("says so plainly where sign-in is not configured", async () => {
    stubDeployment(deployment({ sign_in_available: false }));
    at("/benchmark/beaver");

    await waitFor(() =>
      expect(
        screen.getByText(/this server does not offer sign-in/),
      ).toBeTruthy(),
    );
    expect(screen.queryByRole("link", { name: "Sign in" })).toBeNull();
  });
});
