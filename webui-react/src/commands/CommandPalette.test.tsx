import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { createMemoryHistory, RouterProvider } from "@tanstack/react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

import "../i18n/i18n";
import { createAppRouter } from "../appRouter";
import { execute } from "../graphql/client";
import { AppProviders } from "../providers/AppProviders";

vi.mock("../graphql/client", () => ({
  execute: vi.fn(),
}));

const executeMock = vi.mocked(execute);

async function renderAt(initialEntry: string) {
  const router = createAppRouter({
    history: createMemoryHistory({ initialEntries: [initialEntry] }),
  });

  await router.load();

  render(
    <AppProviders>
      <RouterProvider router={router} />
    </AppProviders>,
  );

  return router;
}

async function openPalette() {
  fireEvent.keyDown(document.body, { key: "k", metaKey: true });

  return screen.findByRole("combobox", { name: "Command palette" });
}

describe("CommandPalette", () => {
  beforeEach(() => {
    executeMock.mockReset();
    executeMock.mockImplementation(() => new Promise<never>(() => undefined));
  });

  it("opens with Cmd or Ctrl+K and closes with Escape", async () => {
    await renderAt("/app/");

    expect(await openPalette()).toBeTruthy();

    fireEvent.keyDown(document, { key: "Escape" });

    await waitFor(() => {
      expect(screen.queryByRole("combobox", { name: "Command palette" })).toBeNull();
    });

    fireEvent.keyDown(document.body, { ctrlKey: true, key: "k" });

    expect(await screen.findByRole("combobox", { name: "Command palette" })).toBeTruthy();
  });

  it("focuses search with slash without hijacking slash inside an input", async () => {
    await renderAt("/app/");

    const searchInput = await screen.findByLabelText("Search torrents");
    document.body.focus();

    fireEvent.keyDown(document.body, { key: "/" });

    expect(document.activeElement).toBe(searchInput);
    expect(screen.queryByRole("combobox", { name: "Command palette" })).toBeNull();

    fireEvent.keyDown(searchInput, { key: "/" });

    expect(document.activeElement).toBe(searchInput);
    expect(screen.queryByRole("combobox", { name: "Command palette" })).toBeNull();
  });

  it("filters commands and enters the active navigation result", async () => {
    const router = await renderAt("/app/");
    const input = await openPalette();

    fireEvent.change(input, { target: { value: "health" } });

    const healthOption = await screen.findByRole("option", { name: "Health" });
    expect(screen.queryByRole("option", { name: "Dashboard" })).toBeNull();

    // The pinned "Search for …" command is highlighted first; arrow down to the nav result.
    fireEvent.keyDown(input, { key: "ArrowDown" });

    expect(input.getAttribute("aria-activedescendant")).toBe(healthOption.id);

    fireEvent.keyDown(input, { key: "Enter" });

    await waitFor(() => {
      expect(router.latestLocation.pathname).toBe("/health");
    });
  });

  it("runs the pinned search command with the typed query", async () => {
    const router = await renderAt("/app/");
    const input = await openPalette();

    fireEvent.change(input, { target: { value: "ubuntu" } });

    expect(
      await screen.findByRole("option", { name: "Search torrents for “ubuntu”" }),
    ).toBeTruthy();

    fireEvent.keyDown(input, { key: "Enter" });

    await waitFor(() => {
      expect(router.latestLocation.pathname).toBe("/");
      expect(router.latestLocation.search).toEqual({ query: "ubuntu" });
    });
  });

  it("connects the combobox to its listbox and active option", async () => {
    await renderAt("/app/");

    const input = await openPalette();
    const listbox = screen.getByRole("listbox");
    const options = within(listbox).getAllByRole("option");

    expect(input.getAttribute("aria-controls")).toBe(listbox.id);
    expect(options.length).toBeGreaterThan(0);

    // The first option is highlighted on open so the default Enter target is visible.
    expect(input.getAttribute("aria-activedescendant")).toBe(options[0]?.id);
    expect(options[0]?.getAttribute("aria-selected")).toBe("true");

    fireEvent.keyDown(input, { key: "ArrowDown" });

    expect(input.getAttribute("aria-activedescendant")).toBe(options[1]?.id);
    expect(options[1]?.getAttribute("aria-selected")).toBe("true");
  });
});
