import type { PropsWithChildren } from "react";
import { createContext, useCallback, useContext, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import styles from "./toast.module.css";

type ToastTone = "info" | "error";

type ToastInput = {
  message: string;
  title?: string;
  tone?: ToastTone;
};

type Toast = Required<ToastInput> & {
  id: string;
};

type Notify = (toast: ToastInput) => void;

const ToastContext = createContext<Notify | null>(null);

function createToastId() {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }

  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

export function ToastProvider({ children }: PropsWithChildren) {
  const [toasts, setToasts] = useState<Toast[]>([]);

  const removeToast = useCallback((id: string) => {
    setToasts((current) => current.filter((toast) => toast.id !== id));
  }, []);

  const notify = useCallback<Notify>(
    ({ message, title = "", tone = "info" }) => {
      const id = createToastId();
      setToasts((current) => [...current, { id, message, title, tone }]);
      window.setTimeout(() => removeToast(id), 4000);
    },
    [removeToast],
  );

  const value = useMemo(() => notify, [notify]);

  return (
    <ToastContext.Provider value={value}>
      {children}
      <ToastViewport onDismiss={removeToast} toasts={toasts} />
    </ToastContext.Provider>
  );
}

export function useToast() {
  const notify = useContext(ToastContext);

  if (!notify) {
    throw new Error("useToast must be used inside ToastProvider");
  }

  return notify;
}

function ToastViewport({
  onDismiss,
  toasts,
}: {
  onDismiss: (id: string) => void;
  toasts: Toast[];
}) {
  const { t } = useTranslation();

  if (toasts.length === 0) {
    return null;
  }

  return (
    <div aria-live="polite" className={styles["viewport"]}>
      {toasts.map((toast) => (
        <div className={styles["toast"]} data-tone={toast.tone} key={toast.id} role="status">
          {toast.title ? <strong>{toast.title}</strong> : null}
          <span>{toast.message}</span>
          <button
            aria-label={t("toast.dismiss")}
            className={styles["dismiss"]}
            onClick={() => onDismiss(toast.id)}
            type="button"
          >
            x
          </button>
        </div>
      ))}
    </div>
  );
}
