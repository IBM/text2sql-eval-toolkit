import { beforeEach, describe, expect, it, vi } from "vitest";

import { SignInRequiredError, apiFetch } from "./api";
import { fetchBenchmarkConfig } from "../services/benchmarks";

/**
 * A 401 that asks the caller to sign in is a different thing from a failure,
 * and the UI can only treat it differently if the error type says so. Both
 * fetch helpers are covered, because both are used for benchmark reads.
 */

function stubResponse(status: number, body: unknown) {
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(JSON.stringify(body), {
          status,
          headers: { "Content-Type": "application/json" },
        }),
    ),
  );
}

beforeEach(() => {
  vi.unstubAllGlobals();
});

describe("sign-in refusals", () => {
  it("apiFetch raises SignInRequiredError for a sign-in refusal", async () => {
    stubResponse(401, { detail: "Sign in to view it.", sign_in_required: true });
    const err = await apiFetch("/api/benchmarks/beaver/summary").catch((e) => e);
    expect(err).toBeInstanceOf(SignInRequiredError);
    expect(err.message).toBe("HTTP 401: Sign in to view it.");
  });

  it("apiFetch keeps a plain Error for any other 401", async () => {
    stubResponse(401, { detail: "Sign-in failed." });
    const err = await apiFetch("/api/auth/callback").catch((e) => e);
    expect(err).toBeInstanceOf(Error);
    expect(err).not.toBeInstanceOf(SignInRequiredError);
  });

  it("the benchmarks service raises it too", async () => {
    stubResponse(401, { detail: "Sign in to view it.", sign_in_required: true });
    const err = await fetchBenchmarkConfig("beaver").catch((e) => e);
    expect(err).toBeInstanceOf(SignInRequiredError);
    expect(err.message).toBe("Sign in to view it.");
  });

  it("a 404 from the benchmarks service is not a sign-in refusal", async () => {
    stubResponse(404, { detail: "Benchmark not found" });
    const err = await fetchBenchmarkConfig("nope").catch((e) => e);
    expect(err).not.toBeInstanceOf(SignInRequiredError);
    expect(err.message).toBe("Benchmark not found");
  });
});
