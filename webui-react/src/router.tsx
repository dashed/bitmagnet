import { lazy, Suspense } from "react";
import {
  Link,
  Outlet,
  RouterProvider,
  createRootRoute,
  createRoute,
  createRouter,
} from "@tanstack/react-router";
import { useTranslation } from "react-i18next";

import { ErrorBoundary } from "./components/ErrorBoundary";
import { ListSkeleton } from "./components/ListSkeleton";
import { QueryError } from "./components/QueryError";
import { AppShell } from "./layout/AppShell";
import { SearchPage } from "./routes/SearchPage";
import { stripTorrentSearchDefaults, validateTorrentSearchParams } from "./routes/searchParams";

const DashboardPage = lazy(() => import("./routes/DashboardPage"));
const TorrentDetailPage = lazy(() => import("./routes/TorrentDetailPage"));

function RootRouteComponent() {
  return (
    <AppShell>
      <ErrorBoundary fallback={({ error, reset }) => <QueryError error={error} onRetry={reset} />}>
        <Outlet />
      </ErrorBoundary>
    </AppShell>
  );
}

function DashboardRouteComponent() {
  return (
    <Suspense fallback={<ListSkeleton ariaLabel="Loading dashboard" rows={4} />}>
      <DashboardPage />
    </Suspense>
  );
}

function TorrentDetailRouteComponent() {
  const { t } = useTranslation();

  return (
    <Suspense fallback={<ListSkeleton ariaLabel={t("detail.loading")} rows={6} />}>
      <TorrentDetailPage />
    </Suspense>
  );
}

function NotFoundPage() {
  const { t } = useTranslation();

  return (
    <div className="route-state" role="status">
      <h1>{t("error.notFound")}</h1>
      <Link to="/">{t("detail.returnToSearch")}</Link>
    </div>
  );
}

const rootRoute = createRootRoute({
  component: RootRouteComponent,
  errorComponent: ({ error }) => <QueryError error={error} />,
  notFoundComponent: NotFoundPage,
});

const searchRoute = createRoute({
  component: SearchPage,
  getParentRoute: () => rootRoute,
  path: "/",
  search: {
    middlewares: [stripTorrentSearchDefaults],
  },
  validateSearch: validateTorrentSearchParams,
});

const dashboardRoute = createRoute({
  component: DashboardRouteComponent,
  getParentRoute: () => rootRoute,
  path: "/dashboard",
});

const torrentDetailRoute = createRoute({
  component: TorrentDetailRouteComponent,
  getParentRoute: () => rootRoute,
  path: "/torrents/$infoHash",
});

const routeTree = rootRoute.addChildren([searchRoute, dashboardRoute, torrentDetailRoute]);

export const router = createRouter({
  basepath: "/app",
  defaultPreload: "intent",
  routeTree,
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

export function AppRouter() {
  return <RouterProvider router={router} />;
}
