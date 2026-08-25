import { expect, test, type Page } from "@playwright/test";

/**
 * The claim Goal 1 makes is: paste a link anywhere and the recipient sees what
 * you saw. Every part of that has unit or component tests except the part that
 * matters — a cold load, in a browser with no prior state, against a real
 * server.
 *
 * So these tests never navigate by clicking twice. They set up a view, take the
 * address out of the browser, open it in a **fresh context** (no storage, no
 * history, no in-memory router state), and compare what renders.
 */

const BENCHMARK = "e2e_demo";
const PIPELINE = "wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi";

/** What a reader would say is "on screen": the visible table, in order. */
async function tableSnapshot(page: Page): Promise<string[]> {
  await expect(page.locator("table tbody tr").first()).toBeVisible();
  return page.locator("table tbody tr").allInnerTexts();
}

/** The pagination line, which is where a lost page number shows up. */
async function paginationLabel(page: Page): Promise<string> {
  const label = page.locator("text=/\\d+–\\d+ of \\d+ items/").first();
  await expect(label).toBeVisible();
  return (await label.innerText()).trim();
}

async function openFresh(page: Page, url: string) {
  await page.goto(url);
  await page.waitForLoadState("networkidle");
}

test.describe("a link reproduces the view", () => {
  test("the error-analysis list, including the page you were on", async ({
    page,
    browser,
  }) => {
    await openFresh(page, `/benchmark/${BENCHMARK}/errors`);

    // Move off page 1: the page number is the piece most easily lost, because
    // it is the one thing not implied by the rest of the state.
    await page.getByRole("button", { name: /next page/i }).click();
    await expect
      .poll(() => page.url())
      .toContain("page=2");

    const url = page.url();
    const expectedRows = await tableSnapshot(page);
    const expectedLabel = await paginationLabel(page);

    // A context, not just a tab: no localStorage, no cookies, no shared
    // router. This is the recipient.
    const recipient = await browser.newContext();
    const opened = await recipient.newPage();
    await openFresh(opened, url);

    expect(await tableSnapshot(opened)).toEqual(expectedRows);
    expect(await paginationLabel(opened)).toBe(expectedLabel);
    await recipient.close();
  });

  test("a filtered list, with the filter still applied", async ({
    page,
    browser,
  }) => {
    const filtered = `/benchmark/${BENCHMARK}/errors?pipeline=${encodeURIComponent(
      PIPELINE
    )}&metric=execution_accuracy&value=0`;
    await openFresh(page, filtered);
    const expectedRows = await tableSnapshot(page);
    const expectedLabel = await paginationLabel(page);

    // The filter has to actually be doing something, or this test would pass
    // against a page that ignored the query string entirely. Compared against a
    // different explicit filter rather than against no filter, because the view
    // applies a default pipeline when the address names none.
    await openFresh(
      page,
      `/benchmark/${BENCHMARK}/errors?pipeline=${encodeURIComponent(
        PIPELINE
      )}&metric=subset_non_empty_execution_accuracy&value=0`
    );
    expect(expectedLabel).not.toBe(await paginationLabel(page));

    const recipient = await browser.newContext();
    const opened = await recipient.newPage();
    await openFresh(opened, filtered);
    expect(await tableSnapshot(opened)).toEqual(expectedRows);
    expect(await paginationLabel(opened)).toBe(expectedLabel);
    await recipient.close();
  });

  test("a pipeline detail page", async ({ page, browser }) => {
    const url = `/benchmark/${BENCHMARK}/pipeline/${encodeURIComponent(PIPELINE)}`;
    await openFresh(page, url);
    await expect(page.getByText(PIPELINE, { exact: false }).first()).toBeVisible();
    const expected = await page.locator("main, .cds--content").first().innerText();

    const recipient = await browser.newContext();
    const opened = await recipient.newPage();
    await openFresh(opened, url);
    expect(
      await opened.locator("main, .cds--content").first().innerText()
    ).toBe(expected);
    await recipient.close();
  });
});

test.describe("the short-link control hands over a working address", () => {
  test("it yields a shorter address for the same view", async ({
    page,
    browser,
  }) => {
    const url = `/benchmark/${BENCHMARK}/pipeline/${encodeURIComponent(PIPELINE)}`;
    await openFresh(page, url);

    await page.getByRole("button", { name: "Copy short link" }).click();
    const short = await page.evaluate(() => navigator.clipboard.readText());

    expect(short.length).toBeLessThan(page.url().length);
    expect(short).not.toContain(encodeURIComponent(PIPELINE));

    const expected = await page.locator("main, .cds--content").first().innerText();

    const recipient = await browser.newContext();
    const opened = await recipient.newPage();
    await openFresh(opened, short);
    // The readable form is canonical, so the alias is expanded on arrival.
    expect(opened.url()).toContain(encodeURIComponent(PIPELINE));
    expect(
      await opened.locator("main, .cds--content").first().innerText()
    ).toBe(expected);
    await recipient.close();
  });

  test("it stays out of the way where it would change nothing", async ({ page }) => {
    // No pipeline in the address means the short form is the address, and a
    // button that copies what is already in the address bar earns nothing.
    await openFresh(page, `/benchmark/${BENCHMARK}/insights`);
    await expect(page.getByText(/Metrics Comparison/i).first()).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Copy short link" })
    ).toHaveCount(0);
  });
});

test.describe("links that do not resolve say so", () => {
  test("an unknown short link is a named not-found, not a blank page", async ({
    page,
  }) => {
    await openFresh(page, `/benchmark/${BENCHMARK}/pipeline/ffffffffff`);
    await expect(page.getByText(/does not name a pipeline/i)).toBeVisible();
  });

  test("an unknown path is a not-found with a way back", async ({ page }) => {
    await openFresh(page, "/benchmark/nope/not-a-view");
    await expect(page.getByRole("button", { name: /go to benchmarks/i })).toBeVisible();
  });
});

test.describe("the browser's own navigation still works", () => {
  test("back returns to the previous view rather than leaving the app", async ({
    page,
  }) => {
    await openFresh(page, `/benchmark/${BENCHMARK}/errors`);
    const first = await paginationLabel(page);

    await page.getByRole("button", { name: /next page/i }).click();
    await expect.poll(() => page.url()).toContain("page=2");
    const second = await paginationLabel(page);
    expect(second).not.toBe(first);

    await page.goBack();
    await expect.poll(() => paginationLabel(page)).toBe(first);
  });

  test("a reload keeps the view instead of dropping to the benchmark list", async ({
    page,
  }) => {
    await openFresh(page, `/benchmark/${BENCHMARK}/insights`);
    const before = page.url();
    await page.reload();
    await page.waitForLoadState("networkidle");
    expect(page.url()).toBe(before);
    await expect(page.getByText(/Metrics Comparison/i).first()).toBeVisible();
  });
});
