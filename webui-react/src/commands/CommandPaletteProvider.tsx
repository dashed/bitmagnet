import {
  createContext,
  lazy,
  Suspense,
  type DependencyList,
  type PropsWithChildren,
  use,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";

import { type Command } from "./types";

type CommandPaletteContextValue = {
  close: () => void;
  isOpen: boolean;
  open: () => void;
  registerCommands: (commands: Command[]) => () => void;
};

const EDITABLE_TAG_NAMES = new Set(["INPUT", "SELECT", "TEXTAREA"]);
const CommandPaletteContext = createContext<CommandPaletteContextValue | null>(null);
const LazyCommandPalette = lazy(() => import("./CommandPalette"));

function isEditableTarget(target: EventTarget | null) {
  return (
    target instanceof HTMLElement &&
    (target.isContentEditable || EDITABLE_TAG_NAMES.has(target.tagName))
  );
}

export function CommandPaletteProvider({ children }: PropsWithChildren) {
  const [commands, setCommands] = useState<Command[]>([]);
  const [isOpen, setIsOpen] = useState(false);

  const close = useCallback(() => setIsOpen(false), []);
  const open = useCallback(() => setIsOpen(true), []);
  const registerCommands = useCallback((nextCommands: Command[]) => {
    if (nextCommands.length === 0) {
      return () => undefined;
    }

    const commandIds = new Set(nextCommands.map((command) => command.id));
    const registeredCommands = new Set(nextCommands);

    setCommands((currentCommands) => [
      ...currentCommands.filter((command) => !commandIds.has(command.id)),
      ...nextCommands,
    ]);

    return () => {
      setCommands((currentCommands) => {
        const remainingCommands = currentCommands.filter(
          (command) => !registeredCommands.has(command),
        );

        return remainingCommands.length === currentCommands.length
          ? currentCommands
          : remainingCommands;
      });
    };
  }, []);

  useEffect(() => {
    function handleKeyDown(event: globalThis.KeyboardEvent) {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        setIsOpen((currentIsOpen) => !currentIsOpen);
        return;
      }

      if (
        event.key !== "/" ||
        event.altKey ||
        event.ctrlKey ||
        event.metaKey ||
        event.shiftKey ||
        isEditableTarget(event.target)
      ) {
        return;
      }

      event.preventDefault();

      const searchInput = document.querySelector<HTMLInputElement>('input[type="search"]');

      if (searchInput) {
        searchInput.focus();
      } else {
        setIsOpen(true);
      }
    }

    document.addEventListener("keydown", handleKeyDown);

    return () => document.removeEventListener("keydown", handleKeyDown);
  }, []);

  const value = useMemo(
    () => ({ close, isOpen, open, registerCommands }),
    [close, isOpen, open, registerCommands],
  );

  return (
    <CommandPaletteContext.Provider value={value}>
      {children}
      {isOpen ? (
        <Suspense fallback={null}>
          <LazyCommandPalette commands={commands} isOpen={isOpen} onClose={close} />
        </Suspense>
      ) : null}
    </CommandPaletteContext.Provider>
  );
}

export function useCommandPalette() {
  const context = use(CommandPaletteContext);

  if (!context) {
    throw new Error("useCommandPalette must be used inside CommandPaletteProvider");
  }

  return context;
}

export function useRegisterCommands(factory: () => Command[], deps: DependencyList) {
  const { registerCommands } = useCommandPalette();

  // The caller owns the dependency list, matching React's memo-hook contract.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const commands = useMemo(factory, deps);

  useEffect(() => registerCommands(commands), [commands, registerCommands]);
}
