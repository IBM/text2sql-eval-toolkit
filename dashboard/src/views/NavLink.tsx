import React from "react";
import { Button } from "@carbon/react";

/**
 * A navigation item that is genuinely a link.
 *
 * These were buttons with onClick handlers, which look the same and behave
 * differently in the ways that matter: no address on hover, nothing to copy,
 * and no "Open in new window" — because a button is not a link, whatever it
 * looks like.
 *
 * Rendering an anchor and intercepting only the plain left click keeps
 * single-page navigation while handing every other gesture back to the browser,
 * which already knows what modifier-click and middle-click mean.
 */

interface Props {
  href: string | null;
  onNavigate: (href: string) => void;
  disabled?: boolean;
  /**
   * Marks this item as the one currently open.
   *
   * Only lists that sit beside their content need it -- the docs view's
   * document list, where "which one am I reading" is not otherwise visible.
   * The main navigation leaves it off: there, the page itself says.
   */
  active?: boolean;
  children: React.ReactNode;
}

export const NavLink: React.FC<Props> = ({
  href,
  onNavigate,
  disabled = false,
  active = false,
  children,
}) => {
  const inert = disabled || !href;

  const handleClick = (event: React.MouseEvent<HTMLElement>) => {
    if (inert) {
      event.preventDefault();
      return;
    }
    // Let the browser have anything that is not a plain left click: cmd or
    // ctrl for a new tab, shift for a new window, alt to download, and any
    // non-primary button. Swallowing these is what makes an SPA feel broken.
    if (
      event.defaultPrevented ||
      event.button !== 0 ||
      event.metaKey ||
      event.ctrlKey ||
      event.shiftKey ||
      event.altKey
    ) {
      return;
    }
    event.preventDefault();
    onNavigate(href as string);
  };

  return (
    <Button
      as="a"
      href={href ?? undefined}
      kind="ghost"
      size="sm"
      disabled={inert}
      // Carbon types Button's handler as a button event even when it renders
      // an anchor; the event itself is the anchor's.
      onClick={handleClick as React.MouseEventHandler<HTMLButtonElement>}
      // aria-current, not only a colour: a screen reader gets the same
      // information the highlight carries.
      aria-current={active ? "page" : undefined}
      style={{
        width: "100%",
        justifyContent: "flex-start",
        ...(active
          ? {
              background: "var(--cds-layer-selected)",
              fontWeight: 600,
            }
          : null),
      }}
    >
      {children}
    </Button>
  );
};
