declare global {
  interface Window {
    /** Set true by the server only for `hermes dashboard --tui` (or HERMES_DASHBOARD_TUI=1). */
    __HERMES_DASHBOARD_EMBEDDED_CHAT__?: boolean;
    /** @deprecated Older injected name; treated as on when true. */
    __HERMES_DASHBOARD_TUI__?: boolean;
  }
}

/** Chat is permanently available in the dashboard; the previous gate on `--tui` flag is removed. */
export function isDashboardEmbeddedChatEnabled(): boolean {
  return true;
}
