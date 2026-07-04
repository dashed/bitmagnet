import { RouterProvider } from "@tanstack/react-router";

import { router } from "./appRouter";

export function AppRouter() {
  return <RouterProvider router={router} />;
}
