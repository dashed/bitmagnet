import { useTranslation } from "react-i18next";

import { SUPPORTED_LANGUAGES, normalizeLanguage, setLanguage } from "../i18n/i18n";
import styles from "./LanguageMenu.module.css";

export function LanguageMenu() {
  const { i18n, t } = useTranslation();
  const currentLanguage = normalizeLanguage(i18n.resolvedLanguage ?? i18n.language);

  return (
    <label className={styles["root"]}>
      <span className={styles["label"]}>{t("language.label")}</span>
      <select
        aria-label={t("language.label")}
        className={styles["select"]}
        onChange={(event) => {
          void setLanguage(event.target.value as (typeof SUPPORTED_LANGUAGES)[number]["value"]);
        }}
        value={currentLanguage}
      >
        {SUPPORTED_LANGUAGES.map((language) => (
          <option key={language.value} value={language.value}>
            {language.label}
          </option>
        ))}
      </select>
    </label>
  );
}
