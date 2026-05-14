export interface SidebarDividerOptions {
  label: string;
}

/**
 * Returns a sidebar item that renders as a visual divider label.
 * Use this in `astro.config.mjs` sidebar groups where you want a manual section break.
 */
export function sidebarDivider({ label }: SidebarDividerOptions) {
  return {
    label,
    link: "#",
    attrs: {
      class: "sl-sidebar-divider",
      "aria-disabled": "true",
      tabindex: -1,
    },
  };
}
