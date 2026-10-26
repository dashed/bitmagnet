import { useComputedColorScheme, useMantineColorScheme } from "@mantine/core";
import { useTranslation } from "react-i18next";

import styles from "./ThemeToggle.module.css";

type ThemeToggleProps = {
  className?: string;
};

function SunIcon() {
  return (
    <svg aria-hidden="true" className={styles["icon"]} viewBox="0 0 24 24">
      <circle cx="12" cy="12" r="4" />
      <path d="M12 2v2M12 20v2M4 12H2M22 12h-2M5 5l1.5 1.5M17.5 17.5 19 19M19 5l-1.5 1.5M6.5 17.5 5 19" />
    </svg>
  );
}

function MoonIcon() {
  return (
    <svg aria-hidden="true" className={styles["icon"]} viewBox="0 0 24 24">
      <path d="M20 15.5A8 8 0 0 1 8.5 4 7 7 0 1 0 20 15.5Z" />
    </svg>
  );
}

export function ThemeToggle({ className }: ThemeToggleProps) {
  const { setColorScheme } = useMantineColorScheme();
  const computedColorScheme = useComputedColorScheme("light", {
    getInitialValueInEffect: false,
  });
  const { t } = useTranslation();

  const nextColorScheme = computedColorScheme === "dark" ? "light" : "dark";
  const label = nextColorScheme === "dark" ? t("theme.switchToDark") : t("theme.switchToLight");

  return (
    <button
      aria-label={label}
      className={[styles["root"], className].filter(Boolean).join(" ")}
      onClick={() => setColorScheme(nextColorScheme)}
      title={label}
      type="button"
    >
      {computedColorScheme === "dark" ? <SunIcon /> : <MoonIcon />}
    </button>
  );
}
