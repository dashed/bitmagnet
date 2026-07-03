import { Link } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import type { PropsWithChildren } from "react";
import { useTranslation } from "react-i18next";

import { LanguageMenu } from "../components/LanguageMenu";
import { ThemeToggle } from "../components/ThemeToggle";
import { execute } from "../graphql/client";
import { VersionDocument } from "../graphql/generated/graphql";
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
  const versionQuery = useQuery({
    queryFn: () => execute(VersionDocument, {}),
    queryKey: ["version"],
  });
  const version = versionQuery.data?.version ?? t("app.version");

  return (
    <div className={styles["root"]}>
      <header className={styles["topBar"]}>
        <Link className={styles["brand"]} to="/">
          <span className={styles["brandName"]}>{t("app.title")}</span>
          <span className={styles["version"]}>{version}</span>
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
