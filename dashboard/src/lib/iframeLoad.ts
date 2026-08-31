/**
 * Telling a cross-origin iframe's real load from its initial `about:blank`.
 *
 * A freshly created iframe fires `load` twice: once for the `about:blank`
 * document the browser puts in it, and again when the real document arrives.
 * Measured against the deployment, those were 3 ms and 480 ms apart on a warm
 * cache, and seconds apart on a cold one. Treating the first as "it has
 * loaded" hides a loading placeholder while the frame is still empty, which is
 * how the docs view came to show a blank white box with nothing to explain it.
 *
 * There is no cross-origin way to ask a frame what it is showing -- but there
 * is a same-origin way to ask whether it is still showing `about:blank`, which
 * is the only document in this sequence that *is* same-origin. Once the frame
 * has navigated to another origin, reading its location throws, and that throw
 * is the signal.
 */

/**
 * Has this frame loaded a real document, as opposed to its initial blank one?
 *
 * Args:
 *     frame: The iframe whose `load` event just fired.
 *
 * Returns:
 *     False while the frame still holds `about:blank`; true once it holds a
 *     document from another origin, and true for a same-origin document that
 *     is not `about:blank`.
 */
export function isRealDocumentLoad(frame: HTMLIFrameElement): boolean {
  try {
    const href = frame.contentWindow?.location.href;
    // Readable, so same-origin. Only the placeholder document should be.
    return href !== "about:blank" && href !== undefined;
  } catch {
    // Unreadable, so cross-origin: the real document is in there.
    return true;
  }
}
