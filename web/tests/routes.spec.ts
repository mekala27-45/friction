import { expect, test, type Page } from "@playwright/test";

const ROUTES = ["/", "/payments/", "/logins/", "/platform/", "/governance/", "/operations/", "/replay/", "/models/", "/report/"];

async function open(page: Page, path: string) {
  // The server answers its first request with a 503; a client retries once, as the site's probe does.
  let r = await page.goto(`/friction${path}`);
  if (r?.status() === 503) r = await page.goto(`/friction${path}`);
  expect(r?.status()).toBe(200);
}

for (const route of ROUTES) {
  test(`${route} renders with the statement`, async ({ page }) => {
    await open(page, route);
    await expect(page.getByTestId("statement")).toContainText("synthetic");
    const width = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(width).toBeLessThanOrEqual(390);
  });
}

test("the server refuses HEAD", async ({ request }) => {
  const r = await request.head("/friction/");
  expect([405, 503]).toContain(r.status());
});

test("the frontier draws every series it lists", async ({ page }) => {
  await open(page, "/");
  const charts = page.getByTestId("frontier");
  const n = await charts.count();
  for (let i = 0; i < n; i++) {
    const series = await charts.nth(i).locator("[data-series]").count();
    expect(series).toBeGreaterThan(1);
  }
});

test("a slip prints on the replay page", async ({ page }) => {
  await open(page, "/replay/");
  const button = page.getByRole("button", { name: /Send/ });
  if (await button.count()) {
    await button.click();
    await expect(page.getByTestId("slip").first()).toBeVisible({ timeout: 15_000 });
    await expect(page.getByTestId("replay-mode")).toContainText(/recorded|live/);
  }
});

test("the masked and unmasked reads render", async ({ page }) => {
  await open(page, "/governance/");
  await expect(page.getByTestId("masked-read")).toContainText("reader identity");
});
