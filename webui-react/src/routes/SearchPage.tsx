import type { FormEvent } from "react";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { useToast } from "../components/toast";
import styles from "./SearchPage.module.css";

export function SearchPage() {
  const [query, setQuery] = useState("");
  const notify = useToast();
  const { t } = useTranslation();

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    notify({
      message: query.trim() || t("search.placeholder"),
      title: t("toast.searchSubmitted"),
    });
  }

  return (
    <section className={styles["root"]}>
      <form className={styles["searchForm"]} onSubmit={handleSubmit} role="search">
        <label className={styles["label"]} htmlFor="torrent-search">
          {t("search.inputLabel")}
        </label>
        <div className={styles["searchControl"]}>
          <input
            autoComplete="off"
            className={styles["input"]}
            id="torrent-search"
            onChange={(event) => setQuery(event.target.value)}
            placeholder={t("search.placeholder")}
            type="search"
            value={query}
          />
          <button className={styles["submit"]} type="submit">
            {t("search.submit")}
          </button>
        </div>
      </form>

      <div className={styles["emptyState"]}>
        <h1>{t("search.emptyTitle")}</h1>
        <p>{t("search.emptyBody")}</p>
      </div>
    </section>
  );
}
