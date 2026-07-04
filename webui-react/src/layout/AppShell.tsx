import { Link, useRouterState } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import type { PropsWithChildren } from "react";
import { useTranslation } from "react-i18next";

import { LanguageMenu } from "../components/LanguageMenu";
import { ThemeToggle } from "../components/ThemeToggle";
import { execute } from "../graphql/client";
import { VersionDocument } from "../graphql/generated/graphql";
import styles from "./AppShell.module.css";

function NavLink({
  active,
  children,
  to,
}: {
  active: boolean;
  children: string;
  to: "/" | "/dashboard";
}) {
  return (
    <Link
      aria-current={active ? "page" : undefined}
      className={[styles["navLink"], active ? styles["navLinkActive"] : ""]
        .filter(Boolean)
        .join(" ")}
      to={to}
    >
      {children}
    </Link>
  );
}

export function AppShell({ children }: PropsWithChildren) {
  const { t } = useTranslation();
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const { data: versionData } = useQuery({
    queryFn: () => execute(VersionDocument, {}),
    queryKey: ["version"],
  });
  const version = versionData?.version ?? t("app.version");
  const torrentsActive =
    pathname === "/" || pathname === "/torrents" || pathname.startsWith("/torrents/");
  const dashboardActive = pathname === "/dashboard";

  return (
    <div className={styles["root"]}>
      <header className={styles["topBar"]}>
        <Link className={styles["brand"]} to="/">
          <span className={styles["brandName"]}>{t("app.title")}</span>
          <span className={styles["version"]}>{version}</span>
        </Link>

        <nav aria-label="Primary" className={styles["desktopNav"]}>
          <NavLink active={torrentsActive} to="/">
            {t("nav.torrents")}
          </NavLink>
          <NavLink active={dashboardActive} to="/dashboard">
            {t("nav.dashboard")}
          </NavLink>
          <a className={styles["navLink"]} href="/?frontend=angular">
            {t("nav.classicUi")}
          </a>
        </nav>

        <div className={styles["actions"]}>
          <LanguageMenu />
          <ThemeToggle />
        </div>
      </header>

      <main className={styles["main"]}>{children}</main>

      <nav aria-label="Primary mobile" className={styles["bottomNav"]}>
        <NavLink active={torrentsActive} to="/">
          {t("nav.torrents")}
        </NavLink>
        <NavLink active={dashboardActive} to="/dashboard">
          {t("nav.dashboard")}
        </NavLink>
      </nav>
    </div>
  );
}
