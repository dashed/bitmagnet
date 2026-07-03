import { fireEvent, render, screen, waitFor } from "@testing-library/react";

import "../i18n/i18n";
import { AppProviders, COLOR_SCHEME_STORAGE_KEY } from "../providers/AppProviders";
import { ThemeToggle } from "./ThemeToggle";

describe("ThemeToggle", () => {
  it("persists the selected color scheme", async () => {
    render(
      <AppProviders>
        <ThemeToggle />
      </AppProviders>,
    );

    fireEvent.click(screen.getByRole("button", { name: /switch to dark theme/i }));

    await waitFor(() => {
      expect(window.localStorage.getItem(COLOR_SCHEME_STORAGE_KEY)).toBe("dark");
      expect(document.documentElement.getAttribute("data-mantine-color-scheme")).toBe("dark");
    });
  });
});
