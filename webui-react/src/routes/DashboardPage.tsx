import { useTranslation } from "react-i18next";

import styles from "./DashboardPage.module.css";

export default function DashboardPage() {
  const { t } = useTranslation();

  return (
    <section className={styles["root"]}>
      <h1>{t("dashboard.title")}</h1>
      <p>{t("dashboard.body")}</p>
    </section>
  );
}
