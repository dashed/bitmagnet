import { MantineProvider, createTheme, localStorageColorSchemeManager } from "@mantine/core";
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

export function AppProviders({ children }: PropsWithChildren) {
  return (
    <MantineProvider
      colorSchemeManager={colorSchemeManager}
      defaultColorScheme="auto"
      theme={theme}
    >
      <ToastProvider>{children}</ToastProvider>
    </MantineProvider>
  );
}
