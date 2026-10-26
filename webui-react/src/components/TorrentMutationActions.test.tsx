import { fireEvent, render, screen, waitFor } from "@testing-library/react";

import "../i18n/i18n";
import { AppProviders } from "../providers/AppProviders";
import { TorrentMutationActions } from "./TorrentMutationActions";

function getDeletePanel(container: HTMLElement) {
  const panel = Array.from(container.querySelectorAll("details")).find(
    (details) => details.querySelector("summary")?.textContent === "Delete",
  );

  if (!panel) {
    throw new Error("Delete panel not found");
  }

  return panel;
}

function getPanelButton(panel: HTMLElement, label: string) {
  const button = Array.from(panel.querySelectorAll("button")).find(
    (candidate) => candidate.textContent === label,
  );

  if (!button) {
    throw new Error(`${label} button not found`);
  }

  return button;
}

describe("TorrentMutationActions", () => {
  it("manages focus inside the delete confirmation dialog", async () => {
    const { container } = render(
      <AppProviders>
        <TorrentMutationActions infoHashes={["0123456789012345678901234567890123456789"]} />
      </AppProviders>,
    );
    const deletePanel = getDeletePanel(container);

    fireEvent.click(deletePanel.querySelector("summary")!);

    const openButton = getPanelButton(deletePanel, "Delete");
    openButton.focus();
    fireEvent.click(openButton);

    const acknowledgeCheckbox = await screen.findByRole("checkbox", {
      name: /i understand this cannot be undone/i,
    });

    await waitFor(() => {
      expect(document.activeElement).toBe(acknowledgeCheckbox);
    });

    fireEvent.keyDown(document, { key: "Tab", shiftKey: true });
    expect(document.activeElement).toBe(screen.getByRole("button", { name: "Cancel" }));

    fireEvent.keyDown(document, { key: "Tab" });
    expect(document.activeElement).toBe(acknowledgeCheckbox);

    fireEvent.keyDown(document, { key: "Escape" });

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });
    expect(document.activeElement).toBe(openButton);

    fireEvent.click(openButton);
    const dialog = await screen.findByRole("dialog");
    const backdrop = dialog.parentElement;

    if (!backdrop) {
      throw new Error("Dialog backdrop not found");
    }

    fireEvent.click(backdrop);

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });
  });
});
