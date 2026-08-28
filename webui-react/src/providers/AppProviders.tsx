import { MantineProvider, createTheme, localStorageColorSchemeManager } from "@mantine/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { PropsWithChildren } from "react";

import { ToastProvider } from "../components/toast";

export const COLOR_SCHEME_STORAGE_KEY = "bitmagnet-color-scheme";

const colorSchemeManager = localStorageColorSchemeManager({
  key: COLOR_SCHEME_STORAGE_KEY,
});

const theme = createTheme({
  defaultRadius: "sm",
  primaryColor: "blue",
});

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: 1,
      staleTime: 30_000,
    },
  },
});

export function AppProviders({ children }: PropsWithChildren) {
  return (
    <QueryClientProvider client={queryClient}>
      <MantineProvider
        colorSchemeManager={colorSchemeManager}
        defaultColorScheme="auto"
        theme={theme}
      >
        <ToastProvider>{children}</ToastProvider>
      </MantineProvider>
    </QueryClientProvider>
  );
}
