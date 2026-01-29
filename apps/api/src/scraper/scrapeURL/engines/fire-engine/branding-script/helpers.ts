// Error tracking
export const errors: Array<{
  context: string;
  message: string;
  timestamp: number;
}> = [];

export const recordError = (context: string, error: unknown) => {
  errors.push({
    context: context,
    message:
      error && (error as Error).message
        ? (error as Error).message
        : String(error),
    timestamp: Date.now(),
  });
};

// Style caching
const styleCache = new WeakMap<Element, CSSStyleDeclaration>();

export const getComputedStyleCached = (el: Element): CSSStyleDeclaration => {
  if (styleCache.has(el)) {
    return styleCache.get(el)!;
  }
  const style = getComputedStyle(el);
  styleCache.set(el, style);
  return style;
};

// Unit conversion
export const toPx = (v: string | null | undefined): number | null => {
  if (!v || v === "auto") return null;
  if (v.endsWith("px")) return parseFloat(v);
  if (v.endsWith("rem"))
    return (
      parseFloat(v) *
      parseFloat(getComputedStyle(document.documentElement).fontSize || "16")
    );
  if (v.endsWith("em"))
    return (
      parseFloat(v) *
      parseFloat(getComputedStyle(document.body).fontSize || "16")
    );
  if (v.endsWith("%")) return null;
  const num = parseFloat(v);
  return Number.isFinite(num) ? num : null;
};

// Class name extraction (handles SVG elements)
export const getClassNameString = (el: Element): string => {
  if (!el || !el.className) return "";
  try {
    const className = el.className as unknown;
    if (className && typeof className === "object" && "baseVal" in className) {
      return String((className as { baseVal: string }).baseVal || "");
    }
    if (typeof className === "string") {
      return className;
    }
    if (
      className &&
      typeof (className as { toString?: unknown }).toString === "function"
    ) {
      return String(className);
    }
    return String(className || "");
  } catch (e) {
    return "";
  }
};
