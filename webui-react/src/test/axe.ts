import axe from "axe-core";

export async function runAxe(container: HTMLElement) {
  const results = await axe.run(container, {
    rules: {
      "color-contrast": { enabled: false },
    },
  });

  return results.violations.filter(
    (violation) => violation.impact === "critical" || violation.impact === "serious",
  );
}

export function formatViolations(violations: Awaited<ReturnType<typeof runAxe>>) {
  return violations.map((violation) => `${violation.id}: ${violation.help}`).join("\n");
}
