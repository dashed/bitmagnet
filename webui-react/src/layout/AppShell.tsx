import { Link } from "@tanstack/react-router";
import type { PropsWithChildren } from "react";
import { useTranslation } from "react-i18next";

import { LanguageMenu } from "../components/LanguageMenu";
import { ThemeToggle } from "../components/ThemeToggle";
import styles from "./AppShell.module.css";

function NavLink({ children, to }: { children: string; to: "/" | "/dashboard" }) {
  return (
    <Link
      activeProps={{
        className: `${styles["navLink"]} ${styles["navLinkActive"]}`,
      }}
      className={styles["navLink"]}
      to={to}
    >
      {children}
    </Link>
  );
}

export function AppShell({ children }: PropsWithChildren) {
  const { t } = useTranslation();

  return (
    <div className={styles["root"]}>
      <header className={styles["topBar"]}>
        <Link className={styles["brand"]} to="/">
          <span className={styles["brandName"]}>{t("app.title")}</span>
          <span className={styles["version"]}>{t("app.version")}</span>
        </Link>

        <nav aria-label="Primary" className={styles["desktopNav"]}>
          <NavLink to="/">{t("nav.torrents")}</NavLink>
          <NavLink to="/dashboard">{t("nav.dashboard")}</NavLink>
        </nav>

        <div className={styles["actions"]}>
          <LanguageMenu />
          <ThemeToggle />
        </div>
      </header>

      <main className={styles["main"]}>{children}</main>

      <nav aria-label="Primary mobile" className={styles["bottomNav"]}>
        <NavLink to="/">{t("nav.torrents")}</NavLink>
        <NavLink to="/dashboard">{t("nav.dashboard")}</NavLink>
      </nav>
    </div>
  );
}
