import { describe, expect, it } from "vitest";

import { parseLocation, routes } from "./routes";

describe("users console route", () => {
  it("builds a shareable address", () => {
    expect(routes.users()).toBe("/users");
  });

  it("round-trips through the parser", () => {
    expect(parseLocation(routes.users()).view).toBe("users");
  });

  it("does not swallow deeper paths", () => {
    // A route that matched anything under /users would capture addresses that
    // belong to a future sub-view.
    expect(parseLocation("/users/someone@example.com").view).not.toBe("users");
  });
});
