import { test, expect } from "@playwright/test";

const base = process.env.BASE_URL || process.env.PLAYWRIGHT_TEST_BASE_URL || "http://localhost:5173";

test.describe("Humble UI smoke", () => {
  test("home loads", async ({ page }) => {
    await page.goto(base + "/");
    await expect(page.getByText(/Humble Library/i)).toBeVisible();
    await expect(page.locator("body")).toHaveScreenshot("home.png", { fullPage: true });
  });

  test("library loads", async ({ page }) => {
    await page.goto(base + "/library");
    await expect(page.getByText(/Library/i)).toBeVisible();
    await expect(page.locator("body")).toHaveScreenshot("library.png", { fullPage: true });
  });

  test("admin loads", async ({ page }) => {
    await page.goto(base + "/admin");
    await expect(page.getByText(/Admin/i)).toBeVisible();
    await expect(page.locator("body")).toHaveScreenshot("admin.png", { fullPage: true });
  });

  test("settings loads", async ({ page }) => {
    await page.goto(base + "/settings");
    await expect(page.getByText(/Settings/i)).toBeVisible();
    await expect(page.locator("body")).toHaveScreenshot("settings.png", { fullPage: true });
  });
});
