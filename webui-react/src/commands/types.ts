export type CommandGroupId =
  | "actions"
  | "language"
  | "navigation"
  | "saved"
  | "search"
  | "theme";

export type Command = {
  group: CommandGroupId;
  hint?: string;
  id: string;
  keywords?: string;
  perform: () => void | Promise<void>;
  title: string;
};
