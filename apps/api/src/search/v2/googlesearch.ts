import { JSDOM } from "jsdom";
import { SearchV2Response, WebSearchResult } from "../../lib/entities";
import { logger } from "../../lib/logger";
import { scrapeURL } from "../../scraper/scrapeURL";
import { scrapeOptions } from "../../controllers/v2/types";
import { CostTracking } from "../../lib/cost-tracking";

function buildInitialSearchURL(
  term: string,
  results: number,
  lang: string,
  country: string,
  tbs?: string,
  filter?: string,
): string {
  const url = new URL("https://www.google.com/search");
  const params = {
    q: term,
    oq: term,
    num: (results + 2).toString(),
    hl: lang,
    gl: country,
    safe: "active",
    start: "0",
    ...(tbs && { tbs }),
    ...(filter && { filter }),
  };

  Object.entries(params).forEach(([key, value]) => {
    url.searchParams.set(key, value);
  });

  return url.toString();
}

function parseSearchResults(html: string): {
  results: WebSearchResult[];
  nextUrl: string | null;
} {
  const dom = new JSDOM(html);
  const document = dom.window.document;

  const nextLink = document.querySelector("a[id=pnnext]") as HTMLAnchorElement;
  const nextUrl = nextLink ? `https://www.google.com${nextLink.href}` : null;

  const rposElements = document.querySelectorAll("div[data-rpos]");
  const results: WebSearchResult[] = [];
  const seenUrls = new Set<string>();

  for (const element of rposElements) {
    const linkTag = element.querySelector("a") as HTMLAnchorElement;
    if (!linkTag) continue;

    const clampDiv = element.querySelector(
      "div[style*='-webkit-line-clamp:2']",
    );
    const descriptionSpan = clampDiv?.querySelector("span");

    if (linkTag && descriptionSpan) {
      const url = linkTag.href;
      const title = linkTag.textContent?.trim() || "";
      const description = descriptionSpan.textContent?.trim() || "";

      if (url && title && description && !seenUrls.has(url)) {
        seenUrls.add(url);
        results.push({ url, title, description });
      }
    }
  }

  return { results, nextUrl };
}

export async function googleSearch(
  term: string,
  advanced = false,
  num_results = 5,
  tbs?: string,
  filter?: string,
  lang = "en",
  country = "us",
  proxy?: string,
  sleep_interval = 0,
  timeout = 5000,
): Promise<SearchV2Response> {
  const allResults: WebSearchResult[] = [];
  let currentUrl: string | null = buildInitialSearchURL(
    term,
    num_results,
    lang,
    country,
    tbs,
    filter,
  );

  let attempts = 0;
  const maxAttempts = 10;

  while (
    currentUrl &&
    allResults.length < num_results &&
    attempts < maxAttempts
  ) {
    try {
      const result = await scrapeURL(
        `google-search:${term}:${num_results}:${lang}:${country}`,
        currentUrl,
        scrapeOptions.parse({ formats: [{ type: "rawHtml" }] }),
        { teamId: "search" },
        new CostTracking(),
      );

      if (!result.success) {
        throw new Error(`Failed to fetch search results: ${result.error}`);
      }

      const html = result.document.rawHtml!;

      const { results, nextUrl } = parseSearchResults(html);

      if (results.length === 0) {
        attempts++;
        currentUrl = nextUrl;
        continue;
      }

      const remainingSlots = num_results - allResults.length;
      allResults.push(...results.slice(0, remainingSlots));

      attempts = 0;

      currentUrl = allResults.length < num_results ? nextUrl : null;

      if (sleep_interval > 0) {
        await new Promise(resolve =>
          setTimeout(resolve, sleep_interval * 1000),
        );
      }
    } catch (error) {
      if (error.message === "Too many requests") {
        logger.warn("Too many requests, stopping search");
        break;
      }
      attempts++;
      logger.error(`Search attempt ${attempts} failed`, { error });

      if (attempts >= maxAttempts) {
        throw error;
      }
    }
  }

  if (attempts >= maxAttempts) {
    logger.warn("Max attempts reached, returning partial results");
  }

  return allResults.length > 0 ? { web: allResults } : {};
}
