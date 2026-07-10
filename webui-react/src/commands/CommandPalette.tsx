import { useComputedColorScheme, useMantineColorScheme } from "@mantine/core";
import { useNavigate } from "@tanstack/react-router";
import {
  type ChangeEvent,
  type KeyboardEvent,
  useEffect,
  useId,
  useMemo,
  useState,
} from "react";
import { useTranslation } from "react-i18next";

import { SUPPORTED_LANGUAGES, setLanguage } from "../i18n/i18n";
import { useSavedSearches } from "../searches/savedSearches";
import { useDialogFocus } from "../utils/dialogFocus";
import styles from "./CommandPalette.module.css";
import { filterCommands } from "./filterCommands";
import { type Command, type CommandGroupId } from "./types";

type CommandPaletteProps = {
  commands: Command[];
  isOpen: boolean;
  onClose: () => void;
};

type GroupedCommands = {
  commands: Command[];
  group: CommandGroupId;
  startIndex: number;
};

function groupCommands(commands: Command[]): GroupedCommands[] {
  const groups = new Map<CommandGroupId, Command[]>();

  for (const command of commands) {
    const groupCommands = groups.get(command.group);

    if (groupCommands) {
      groupCommands.push(command);
    } else {
      groups.set(command.group, [command]);
    }
  }

  let startIndex = 0;

  return Array.from(groups, ([group, groupItems]) => {
    const groupedCommands = {
      commands: groupItems,
      group,
      startIndex,
    };
    startIndex += groupItems.length;

    return groupedCommands;
  });
}

export default function CommandPalette({ commands, isOpen, onClose }: CommandPaletteProps) {
  const [activeIndex, setActiveIndex] = useState(0);
  const [query, setQuery] = useState("");
  const { setColorScheme } = useMantineColorScheme();
  const computedColorScheme = useComputedColorScheme("light", {
    getInitialValueInEffect: false,
  });
  const dialogRef = useDialogFocus(isOpen, onClose);
  const listboxId = useId();
  const navigate = useNavigate();
  const savedSearches = useSavedSearches();
  const { t } = useTranslation();

  const navigationCommands = useMemo<Command[]>(
    () => [
      {
        group: "navigation",
        id: "navigation-search",
        perform: () => navigate({ to: "/" }),
        title: t("nav.torrents"),
      },
      {
        group: "navigation",
        id: "navigation-dashboard",
        perform: () => navigate({ to: "/dashboard" }),
        title: t("nav.dashboard"),
      },
      {
        group: "navigation",
        id: "navigation-queue",
        perform: () => navigate({ to: "/queue" }),
        title: t("queue.title"),
      },
      {
        group: "navigation",
        id: "navigation-health",
        perform: () => navigate({ to: "/health" }),
        title: t("health.title"),
      },
      {
        group: "navigation",
        hint: t("palette.external"),
        id: "navigation-classic-ui",
        perform: () => window.location.assign("/?frontend=angular"),
        title: t("nav.classicUi"),
      },
    ],
    [navigate, t],
  );
  const themeCommand = useMemo<Command>(() => {
    const nextColorScheme = computedColorScheme === "dark" ? "light" : "dark";

    return {
      group: "theme",
      id: "theme-toggle",
      perform: () => setColorScheme(nextColorScheme),
      title: nextColorScheme === "dark" ? t("theme.switchToDark") : t("theme.switchToLight"),
    };
  }, [computedColorScheme, setColorScheme, t]);
  const languageCommands = useMemo<Command[]>(
    () =>
      SUPPORTED_LANGUAGES.map((language) => ({
        group: "language",
        id: `language-${language.value}`,
        perform: () => {
          void setLanguage(language.value);
        },
        title: t("palette.language", { language: language.label }),
      })),
    [t],
  );
  const searchCommand = useMemo<Command | null>(() => {
    const trimmedQuery = query.trim();

    if (!trimmedQuery) {
      return null;
    }

    return {
      group: "search",
      id: "search-query",
      perform: () =>
        navigate({
          search: { query: trimmedQuery },
          to: "/",
        }),
      title: t("palette.searchFor", { query: trimmedQuery }),
    };
  }, [navigate, query, t]);
  const savedSearchCommands = useMemo<Command[]>(
    () =>
      savedSearches.map((item) => ({
        group: "saved",
        hint: t("savedSearches.paletteHint"),
        id: `saved-search-${item.id}`,
        perform: () =>
          navigate({
            search: item.params,
            to: "/",
          }),
        title: item.name,
      })),
    [navigate, savedSearches, t],
  );
  const availableCommands = useMemo(
    () => [
      ...navigationCommands,
      ...(searchCommand ? [searchCommand] : []),
      ...savedSearchCommands,
      ...commands,
      themeCommand,
      ...languageCommands,
    ],
    [
      commands,
      languageCommands,
      navigationCommands,
      savedSearchCommands,
      searchCommand,
      themeCommand,
    ],
  );
  const filteredCommands = useMemo(
    () => filterCommands(availableCommands, query),
    [availableCommands, query],
  );
  const commandGroups = useMemo(() => groupCommands(filteredCommands), [filteredCommands]);
  const visibleCommands = useMemo(
    () => commandGroups.flatMap((group) => group.commands),
    [commandGroups],
  );
  const activeOptionId =
    activeIndex >= 0 && visibleCommands[activeIndex]
      ? `${listboxId}-${activeIndex}`
      : undefined;

  useEffect(() => {
    if (isOpen) {
      setActiveIndex(0);
      setQuery("");
    }
  }, [isOpen]);

  useEffect(() => {
    setActiveIndex((currentIndex) => {
      if (visibleCommands.length === 0) {
        return -1;
      }

      if (currentIndex < 0) {
        return 0;
      }

      return currentIndex >= visibleCommands.length ? visibleCommands.length - 1 : currentIndex;
    });
  }, [visibleCommands.length]);

  useEffect(() => {
    if (!activeOptionId) {
      return;
    }

    document.getElementById(activeOptionId)?.scrollIntoView?.({ block: "nearest" });
  }, [activeOptionId]);

  function performCommand(command: Command | undefined) {
    if (!command) {
      return;
    }

    try {
      void command.perform();
    } finally {
      onClose();
    }
  }

  function handleInputChange(event: ChangeEvent<HTMLInputElement>) {
    setActiveIndex(0);
    setQuery(event.target.value);
  }

  function handleInputKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setActiveIndex((currentIndex) =>
        visibleCommands.length === 0 ? -1 : (currentIndex + 1) % visibleCommands.length,
      );
      return;
    }

    if (event.key === "ArrowUp") {
      event.preventDefault();
      setActiveIndex((currentIndex) => {
        if (visibleCommands.length === 0) {
          return -1;
        }

        return currentIndex <= 0 ? visibleCommands.length - 1 : currentIndex - 1;
      });
      return;
    }

    if (event.key === "Home") {
      event.preventDefault();
      setActiveIndex(visibleCommands.length === 0 ? -1 : 0);
      return;
    }

    if (event.key === "End") {
      event.preventDefault();
      setActiveIndex(visibleCommands.length - 1);
      return;
    }

    if (event.key === "Enter") {
      event.preventDefault();
      performCommand(visibleCommands[activeIndex >= 0 ? activeIndex : 0]);
    }
  }

  if (!isOpen) {
    return null;
  }

  return (
    <div
      className={styles["backdrop"]}
      onClick={(event) => {
        if (event.target === event.currentTarget) {
          onClose();
        }
      }}
      role="presentation"
    >
      <div
        aria-label={t("palette.inputLabel")}
        aria-modal="true"
        className={styles["palette"]}
        ref={dialogRef}
        role="dialog"
        tabIndex={-1}
      >
        <input
          aria-activedescendant={activeOptionId}
          aria-autocomplete="list"
          aria-controls={listboxId}
          aria-expanded={true}
          aria-label={t("palette.inputLabel")}
          autoComplete="off"
          className={styles["input"]}
          onChange={handleInputChange}
          onKeyDown={handleInputKeyDown}
          placeholder={t("palette.hint")}
          role="combobox"
          type="text"
          value={query}
        />

        <div className={styles["list"]} id={listboxId} role="listbox">
          {visibleCommands.length > 0 ? (
            commandGroups.map((commandGroup) => (
              <div
                className={styles["group"]}
                key={commandGroup.group}
                role="presentation"
              >
                <div className={styles["groupHeader"]} role="presentation">
                  {t(`palette.groups.${commandGroup.group}`)}
                </div>
                {commandGroup.commands.map((command, index) => {
                  const optionIndex = commandGroup.startIndex + index;

                  return (
                    <div
                      aria-selected={optionIndex === activeIndex}
                      className={styles["option"]}
                      id={`${listboxId}-${optionIndex}`}
                      key={command.id}
                      onClick={() => performCommand(command)}
                      onMouseDown={(event) => event.preventDefault()}
                      role="option"
                    >
                      <span className={styles["optionTitle"]}>{command.title}</span>
                      {command.hint ? (
                        <span className={styles["hint"]}>{command.hint}</span>
                      ) : null}
                    </div>
                  );
                })}
              </div>
            ))
          ) : (
            <div className={styles["empty"]}>{t("palette.empty")}</div>
          )}
        </div>
      </div>
    </div>
  );
}
