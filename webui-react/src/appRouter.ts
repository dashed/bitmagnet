import { createRootRoute, createRoute, createRouter, redirect } from "@tanstack/react-router";
import type { RouterHistory } from "@tanstack/react-router";

import {
  DashboardRouteComponent,
  NotFoundPage,
  RootErrorComponent,
  RootRouteComponent,
  TorrentDetailRouteComponent,
  QueueRouteComponent,
  HealthRouteComponent,
} from "./routeComponents";
import { SearchPage } from "./routes/SearchPage";
import {
  normalizeLegacyTorrentSearch,
  stripTorrentSearchDefaults,
  validateTorrentSearchParams,
} from "./routes/searchParams";

type AppRouterOptions = {
  history?: RouterHistory;
};

const rootRoute = createRootRoute({
  component: RootRouteComponent,
  errorComponent: RootErrorComponent,
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

const queueRoute = createRoute({
  component: QueueRouteComponent,
  getParentRoute: () => rootRoute,
  path: "/queue",
});

const healthRoute = createRoute({
  component: HealthRouteComponent,
  getParentRoute: () => rootRoute,
  path: "/health",
});

const routeTree = rootRoute.addChildren([
  searchRoute,
  legacyTorrentsSearchRoute,
  dashboardRoute,
  torrentPermalinkRoute,
  torrentDetailRoute,
  queueRoute,
  healthRoute,
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
