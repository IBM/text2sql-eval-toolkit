import React from "react";
import { Launch } from "@carbon/icons-react";
import "./LinkTile.css";

/**
 * A tile that is a link.
 *
 * An anchor rather than a div with a click handler: these navigate, and a link
 * is what makes "open in a new tab", middle-click and copy-address work. The
 * plain left click is intercepted for single-page navigation and every other
 * gesture is handed back to the browser -- the same bargain `NavLink` strikes.
 *
 * Shared by the home page and the documentation index, which is why it lives
 * here rather than inside either of them.
 */
interface Props {
  /** Small label above the title, saying what kind of thing this is. */
  eyebrow: string;
  title: string;
  summary?: string;
  /** `null` renders the tile inert -- see `unavailableReason`. */
  href: string | null;
  /** Opens in a new tab, and is marked as doing so. */
  external?: boolean;
  onNavigate?: (href: string) => void;
  /**
   * Shown in place of the summary when `href` is null.
   *
   * A tile that cannot be followed is still worth showing: it says the view
   * exists and what it needs. Hiding it would leave the reader wondering
   * whether the dashboard has the feature at all.
   */
  unavailableReason?: string;
}

export const LinkTile: React.FC<Props> = ({
  eyebrow,
  title,
  summary,
  href,
  external = false,
  onNavigate,
  unavailableReason,
}) => {
  const body = (
    <>
      <span className="t2s-tile__eyebrow">
        {eyebrow}
        {external && <Launch size={14} aria-label="opens in a new tab" />}
      </span>
      <span className="t2s-tile__title">{title}</span>
      {(href ? summary : (unavailableReason ?? summary)) && (
        <span className="t2s-tile__summary">
          {href ? summary : (unavailableReason ?? summary)}
        </span>
      )}
    </>
  );

  if (!href) {
    return (
      <div className="t2s-tile t2s-tile--inert" aria-disabled="true">
        {body}
      </div>
    );
  }

  return (
    <a
      className="t2s-tile"
      href={href}
      {...(external
        ? { target: "_blank", rel: "noopener noreferrer" }
        : {
            onClick: (event: React.MouseEvent<HTMLAnchorElement>) => {
              if (
                !onNavigate ||
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
              onNavigate(href);
            },
          })}
    >
      {body}
    </a>
  );
};

/** The grid these sit in. Shared so the two pages cannot drift apart. */
export const TileGrid: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => <div className="t2s-tile-grid">{children}</div>;
