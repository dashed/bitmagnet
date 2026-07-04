import { lazy, Suspense } from "react";
import {
  Link,
  Outlet,
  RouterProvider,
  createRootRoute,
  createRoute,
  createRouter,
  redirect,
} from "@tanstack/react-router";
import type { RouterHistory } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";

import { ErrorBoundary } from "./components/ErrorBoundary";
import { ListSkeleton } from "./components/ListSkeleton";
import { QueryError } from "./components/QueryError";
import { AppShell } from "./layout/AppShell";
import { SearchPage } from "./routes/SearchPage";
import {
  normalizeLegacyTorrentSearch,
  stripTorrentSearchDefaults,
  validateTorrentSearchParams,
} from "./routes/searchParams";

const DashboardPage = lazy(() => import("./routes/DashboardPage"));
const TorrentDetailPage = lazy(() => import("./routes/TorrentDetailPage"));

type AppRouterOptions = {
  history?: RouterHistory;
};

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

function redirectLegacyTorrentSearch(search: unknown) {
  const normalization = normalizeLegacyTorrentSearch(search);

  if (normalization.kind === "detail") {
    redirect({
      params: { infoHash: normalization.infoHash },
      replace: true,
      search: {},
      throw: true,
      to: "/torrents/$infoHash",
    });
  }

  if (normalization.kind === "search") {
    redirect({
      replace: true,
      search: normalization.search,
      throw: true,
      to: "/",
    });
  }
}

const searchRoute = createRoute({
  beforeLoad: ({ location }) => redirectLegacyTorrentSearch(location.search),
  component: SearchPage,
  getParentRoute: () => rootRoute,
  path: "/",
  search: {
    middlewares: [stripTorrentSearchDefaults],
  },
  validateSearch: validateTorrentSearchParams,
});

const legacyTorrentsSearchRoute = createRoute({
  beforeLoad: ({ location }) => {
    redirectLegacyTorrentSearch(location.search);

    redirect({
      replace: true,
      search: validateTorrentSearchParams(location.search),
      throw: true,
      to: "/",
    });
  },
  getParentRoute: () => rootRoute,
  path: "/torrents",
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

const torrentPermalinkRoute = createRoute({
  beforeLoad: ({ params }) => {
    redirect({
      params: { infoHash: params.infoHash },
      replace: true,
      search: {},
      throw: true,
      to: "/torrents/$infoHash",
    });
  },
  getParentRoute: () => rootRoute,
  path: "/torrents/permalink/$infoHash",
});

const torrentDetailRoute = createRoute({
  component: TorrentDetailRouteComponent,
  getParentRoute: () => rootRoute,
  path: "/torrents/$infoHash",
});

const routeTree = rootRoute.addChildren([
  searchRoute,
  legacyTorrentsSearchRoute,
  dashboardRoute,
  torrentPermalinkRoute,
  torrentDetailRoute,
]);

export function createAppRouter(options: AppRouterOptions = {}) {
  return createRouter({
    basepath: "/app",
    defaultPreload: "intent",
    history: options.history,
    routeTree,
  });
}

export const router = createAppRouter();

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

export function AppRouter() {
  return <RouterProvider router={router} />;
}
