import { Alert } from "@mantine/core";
import { useTranslation } from "react-i18next";

import styles from "./QueryError.module.css";

type QueryErrorProps = {
  error: unknown;
  onRetry?: () => void;
};

function getErrorMessage(error: unknown) {
  if (error instanceof Error) {
    return error.message;
  }

  if (typeof error === "string") {
    return error;
  }

  return null;
}

export function QueryError({ error, onRetry }: QueryErrorProps) {
  const { t } = useTranslation();
  const message = getErrorMessage(error) ?? t("error.title");

  return (
    <Alert color="red" radius="sm" title={t("error.title")} variant="light">
      <div className={styles["content"]}>
        <p>{message}</p>
        {onRetry ? (
          <button className={styles["retryButton"]} onClick={onRetry} type="button">
            {t("error.retry")}
          </button>
        ) : null}
      </div>
    </Alert>
  );
}
