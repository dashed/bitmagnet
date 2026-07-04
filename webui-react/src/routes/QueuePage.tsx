import { useTranslation } from "react-i18next";

import { QueueAdminSection } from "./queue/QueueAdminSection";
import { QueueJobsSection } from "./queue/QueueJobsSection";
import { QueueMetricsSection } from "./queue/QueueMetricsSection";
import styles from "./QueuePage.module.css";

export default function QueuePage() {
  const { t } = useTranslation();

  return (
    <div className={styles["root"]}>
      <header className={styles["header"]}>
        <div>
          <h1>{t("queue.title", "Queue")}</h1>
          <p>
            {t(
              "queue.body",
              "Monitor queue throughput, inspect jobs, and run scoped purge actions.",
            )}
          </p>
        </div>
        <nav aria-label={t("queue.sections", "Queue sections")} className={styles["sectionNav"]}>
          <a href="#queue-visualize">{t("queue.nav.visualize", "Visualize")}</a>
          <a href="#queue-jobs">{t("queue.nav.jobs", "Jobs")}</a>
          <a href="#queue-admin">{t("queue.nav.admin", "Admin")}</a>
        </nav>
      </header>

      <QueueMetricsSection />
      <QueueJobsSection />
      <QueueAdminSection />
    </div>
  );
}
