import { act, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { useDebouncedValue } from "./debounce";
import { TAG_SUGGEST_DEBOUNCE_MS } from "./torrentMutationActions";

function DebouncedProbe({ value }: { value: string }) {
  const debouncedValue = useDebouncedValue(value, TAG_SUGGEST_DEBOUNCE_MS);

  return <output aria-label="debounced tag input">{debouncedValue}</output>;
}

describe("useDebouncedValue", () => {
  it("debounces tag suggestion input by 300ms", () => {
    vi.useFakeTimers();

    try {
      const { rerender } = render(<DebouncedProbe value="mov" />);

      expect(screen.getByLabelText("debounced tag input").textContent).toBe("mov");

      rerender(<DebouncedProbe value="movie" />);

      expect(screen.getByLabelText("debounced tag input").textContent).toBe("mov");

      act(() => {
        vi.advanceTimersByTime(TAG_SUGGEST_DEBOUNCE_MS - 1);
      });

      expect(screen.getByLabelText("debounced tag input").textContent).toBe("mov");

      act(() => {
        vi.advanceTimersByTime(1);
      });

      expect(screen.getByLabelText("debounced tag input").textContent).toBe("movie");
    } finally {
      vi.useRealTimers();
    }
  });
});
