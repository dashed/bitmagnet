import { lazy, Suspense } from "react";
import { Link, Outlet } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";

import { ErrorBoundary } from "./components/ErrorBoundary";
import { ListSkeleton } from "./components/ListSkeleton";
import { QueryError } from "./components/QueryError";
import { AppShell } from "./layout/AppShell";

const DashboardPage = lazy(() => import("./routes/DashboardPage"));
const TorrentDetailPage = lazy(() => import("./routes/TorrentDetailPage"));
const QueuePage = lazy(() => import("./routes/QueuePage"));
const HealthPage = lazy(() => import("./routes/HealthPage"));

export function RootRouteComponent() {
  return (
    <AppShell>
      <ErrorBoundary fallback={({ error, reset }) => <QueryError error={error} onRetry={reset} />}>
        <Outlet />
      </ErrorBoundary>
    </AppShell>
  );
}

export function RootErrorComponent({ error }: { error: unknown }) {
  return <QueryError error={error} />;
}

export function DashboardRouteComponent() {
  return (
    <Suspense fallback={<ListSkeleton ariaLabel="Loading dashboard" rows={4} />}>
      <DashboardPage />
    </Suspense>
  );
}

export function TorrentDetailRouteComponent() {
  const { t } = useTranslation();

  return (
    <Suspense fallback={<ListSkeleton ariaLabel={t("detail.loading")} rows={6} />}>
      <TorrentDetailPage />
    </Suspense>
  );
}

export function NotFoundPage() {
  const { t } = useTranslation();

  return (
    <div className="route-state" role="status">
      <h1>{t("error.notFound")}</h1>
      <Link to="/">{t("detail.returnToSearch")}</Link>
    </div>
  );
}

export function QueueRouteComponent() {
  return (
    <Suspense fallback={null}>
      <QueuePage />
    </Suspense>
  );
}

export function HealthRouteComponent() {
  return (
    <Suspense fallback={null}>
      <HealthPage />
    </Suspense>
  );
}
