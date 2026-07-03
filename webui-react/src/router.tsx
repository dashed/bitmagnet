import { lazy, Suspense } from "react";
import {
  Link,
  Outlet,
  RouterProvider,
  createRootRoute,
  createRoute,
  createRouter,
} from "@tanstack/react-router";

import { ErrorBoundary } from "./components/ErrorBoundary";
import { ListSkeleton } from "./components/ListSkeleton";
import { QueryError } from "./components/QueryError";
import { AppShell } from "./layout/AppShell";
import { SearchPage } from "./routes/SearchPage";

const DashboardPage = lazy(() => import("./routes/DashboardPage"));

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

function NotFoundPage() {
  return (
    <div className="route-state" role="status">
      <h1>Not found</h1>
      <Link to="/">Return to torrents</Link>
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
});

const dashboardRoute = createRoute({
  component: DashboardRouteComponent,
  getParentRoute: () => rootRoute,
  path: "/dashboard",
});

const routeTree = rootRoute.addChildren([searchRoute, dashboardRoute]);

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
