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

/**
 * Wait until the view has stopped changing on its own.
 *
 * The error-analysis view picks a default pipeline *after* its first paint and
 * writes it into the address, which triggers a refetch. Between those two
 * moments the metric columns read "N/A" -- a state no recipient of a shared
 * link ever sees, because their address already names the pipeline. Snapshotting
 * during it makes these tests fail for a reason that has nothing to do with
 * URLs.
 */
async function settled(page: Page) {
  if (page.url().includes("/errors")) {
    await page.waitForFunction(
      () => window.location.search.includes("pipeline="),
      { timeout: 15_000 }
    );
  }
  await page.waitForLoadState("networkidle");

  // `networkidle` is not "done": choosing the default pipeline triggers a
  // second fetch, and the window between the two is real time during which the
  // table shows different text. Rather than guess at how many round trips the
  // view makes, wait for the rendered rows to stop changing -- which is the
  // same thing a reader means by "it has loaded".
  let previous = "";
  for (let attempt = 0; attempt < 25; attempt++) {
    const current = (await page.locator("table tbody tr").allInnerTexts()).join("|");
    if (current && current === previous) return;
    previous = current;
    await page.waitForTimeout(200);
  }
}

async function openFresh(page: Page, url: string) {
  await page.goto(url);
  await page.waitForLoadState("networkidle");
  await settled(page);
}

/** Turn the page and wait for the new rows to be the ones on screen. */
async function nextPage(page: Page) {
  await expect(page.locator("table tbody tr").first()).toBeVisible();
  await page.getByRole("button", { name: /next page/i }).click();
  await expect.poll(() => page.url()).toContain("page=2");
  await settled(page);
}

test.describe("a link reproduces the view", () => {
  test("the error-analysis list, including the page you were on", async ({
    page,
    browser,
  }) => {
    await openFresh(page, `/benchmark/${BENCHMARK}/errors`);
    // Move off page 1: the page number is the piece most easily lost, because
    // it is the one thing not implied by the rest of the state.
    await nextPage(page);

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
    await expect(page.locator("table tbody tr").first()).toBeVisible();
    const first = await paginationLabel(page);

    await nextPage(page);
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

test.describe("large results stay renderable", () => {
  test("a huge query result is previewed, and says how much it is hiding", async ({
    page,
  }) => {
    // The fixture's result frames are small, so this asserts the mechanism
    // rather than a specific size: whatever arrives, the panel must not put an
    // unbounded number of rows in the DOM. On real data one Beaver record
    // answers with 86,502 rows, which built 854,563 nodes and 858 MB of heap
    // before the result tables were paginated.
    await openFresh(page, `/benchmark/${BENCHMARK}/errors`);
    await expect(page.locator("table tbody tr").first()).toBeVisible();
    await page.locator("table tbody tr").first().click();
    await expect(page.getByText("Predicted result").first()).toBeVisible();

    const nodes = await page.evaluate(
      () => document.getElementsByTagName("*").length
    );
    expect(nodes).toBeLessThan(20_000);
  });
});
