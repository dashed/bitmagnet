import { Skeleton } from "@mantine/core";
import { useTranslation } from "react-i18next";

import styles from "./ListSkeleton.module.css";

type ListSkeletonProps = {
  ariaLabel?: string;
  rows?: number;
};

export function ListSkeleton({ ariaLabel, rows = 5 }: ListSkeletonProps) {
  const { t } = useTranslation();

  return (
    <div
      aria-label={ariaLabel ?? t("error.loading")}
      aria-live="polite"
      className={styles["root"]}
      role="status"
    >
      {Array.from({ length: rows }, (_, index) => (
        <Skeleton className={styles["row"]} height={52} key={index} radius="sm" />
      ))}
    </div>
  );
}
