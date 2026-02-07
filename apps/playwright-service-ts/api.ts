import express, { Request, Response } from 'express';
import { chromium, Browser, BrowserContext, Route, Request as PlaywrightRequest, Page } from 'playwright';
import dotenv from 'dotenv';
import UserAgent from 'user-agents';
import { getError } from './helpers/get_error';

dotenv.config();

const app = express();
const port = process.env.PORT || 3003;

app.use(express.json());

const BLOCK_MEDIA = (process.env.BLOCK_MEDIA || 'False').toUpperCase() === 'TRUE';
const MAX_CONCURRENT_PAGES = Math.max(1, Number.parseInt(process.env.MAX_CONCURRENT_PAGES ?? '10', 10) || 10);

const PROXY_SERVER = process.env.PROXY_SERVER || null;
const PROXY_USERNAME = process.env.PROXY_USERNAME || null;
const PROXY_PASSWORD = process.env.PROXY_PASSWORD || null;
const CF_PROXY_URL = process.env.CF_PROXY_URL || null; // e.g. https://scribe-proxy.xorqq.workers.dev
const RESIDENTIAL_PROXY_SERVER = process.env.RESIDENTIAL_PROXY_SERVER || null;
const RESIDENTIAL_PROXY_USERNAME = process.env.RESIDENTIAL_PROXY_USERNAME || null;
const RESIDENTIAL_PROXY_PASSWORD = process.env.RESIDENTIAL_PROXY_PASSWORD || null;
class Semaphore {
  private permits: number;
  private queue: (() => void)[] = [];

  constructor(permits: number) {
    this.permits = permits;
  }

  async acquire(): Promise<void> {
    if (this.permits > 0) {
      this.permits--;
      return Promise.resolve();
    }

    return new Promise<void>((resolve) => {
      this.queue.push(resolve);
    });
  }

  release(): void {
    this.permits++;
    if (this.queue.length > 0) {
      const nextResolve = this.queue.shift();
      if (nextResolve) {
        this.permits--;
        nextResolve();
      }
    }
  }

  getAvailablePermits(): number {
    return this.permits;
  }

  getQueueLength(): number {
    return this.queue.length;
  }
}
const pageSemaphore = new Semaphore(MAX_CONCURRENT_PAGES);

const AD_SERVING_DOMAINS = [
  'doubleclick.net',
  'adservice.google.com',
  'googlesyndication.com',
  'googletagservices.com',
  'googletagmanager.com',
  'google-analytics.com',
  'adsystem.com',
  'adservice.com',
  'adnxs.com',
  'ads-twitter.com',
  'facebook.net',
  'fbcdn.net',
  'amazon-adsystem.com'
];

interface UrlModel {
  url: string;
  wait_after_load?: number;
  timeout?: number;
  headers?: { [key: string]: string };
  check_selector?: string;
  skip_tls_verification?: boolean;
  screenshot?: boolean;
  screenshot_full_page?: boolean;
}

let browser: Browser;

const initializeBrowser = async () => {
  browser = await chromium.launch({
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--no-first-run',
      '--no-zygote',
      '--disable-gpu'
    ]
  });
};

const createContext = async (skipTlsVerification: boolean = false) => {
  const userAgent = new UserAgent().toString();
  const viewport = { width: 1280, height: 800 };

  const contextOptions: any = {
    userAgent,
    viewport,
    ignoreHTTPSErrors: skipTlsVerification,
  };

  if (PROXY_SERVER && PROXY_USERNAME && PROXY_PASSWORD) {
    contextOptions.proxy = {
      server: PROXY_SERVER,
      username: PROXY_USERNAME,
      password: PROXY_PASSWORD,
    };
  } else if (PROXY_SERVER) {
    contextOptions.proxy = {
      server: PROXY_SERVER,
    };
  }

  const newContext = await browser.newContext(contextOptions);

  if (BLOCK_MEDIA) {
    await newContext.route('**/*.{png,jpg,jpeg,gif,svg,mp3,mp4,avi,flac,ogg,wav,webm}', async (route: Route, request: PlaywrightRequest) => {
      await route.abort();
    });
  }

  // Intercept all requests to avoid loading ads
  await newContext.route('**/*', (route: Route, request: PlaywrightRequest) => {
    const requestUrl = new URL(request.url());
    const hostname = requestUrl.hostname;

    if (AD_SERVING_DOMAINS.some(domain => hostname.includes(domain))) {
      console.log(hostname);
      return route.abort();
    }
    return route.continue();
  });
  
  return newContext;
};

// Create a context with the residential proxy for DataDome/paywall bypass.
// Uses direct navigation (no CF proxy URL rewriting) so cookies work on the real domain.
const createResidentialContext = async (skipTlsVerification: boolean = false) => {
  if (!RESIDENTIAL_PROXY_SERVER) return null;

  const userAgent = new UserAgent().toString();
  const contextOptions: any = {
    userAgent,
    viewport: { width: 1280, height: 800 },
    ignoreHTTPSErrors: skipTlsVerification,
    proxy: {
      server: RESIDENTIAL_PROXY_SERVER,
      ...(RESIDENTIAL_PROXY_USERNAME && { username: RESIDENTIAL_PROXY_USERNAME }),
      ...(RESIDENTIAL_PROXY_PASSWORD && { password: RESIDENTIAL_PROXY_PASSWORD }),
    },
  };

  const ctx = await browser.newContext(contextOptions);

  if (BLOCK_MEDIA) {
    await ctx.route('**/*.{png,jpg,jpeg,gif,svg,mp3,mp4,avi,flac,ogg,wav,webm}', async (route: Route) => {
      await route.abort();
    });
  }

  await ctx.route('**/*', (route: Route, request: PlaywrightRequest) => {
    const hostname = new URL(request.url()).hostname;
    if (AD_SERVING_DOMAINS.some(domain => hostname.includes(domain))) {
      return route.abort();
    }
    return route.continue();
  });

  return ctx;
};

const shutdownBrowser = async () => {
  if (browser) {
    await browser.close();
  }
};

const isValidUrl = (urlString: string): boolean => {
  try {
    new URL(urlString);
    return true;
  } catch (_) {
    return false;
  }
};

const scrapePage = async (page: Page, url: string, waitUntil: 'load' | 'networkidle', waitAfterLoad: number, timeout: number, checkSelector: string | undefined) => {
  console.log(`Navigating to ${url} with waitUntil: ${waitUntil} and timeout: ${timeout}ms`);
  const response = await page.goto(url, { waitUntil, timeout });

  if (waitAfterLoad > 0) {
    await page.waitForTimeout(waitAfterLoad);
  }

  if (checkSelector) {
    try {
      await page.waitForSelector(checkSelector, { timeout });
    } catch (error) {
      throw new Error('Required selector not found');
    }
  }

  let headers = null, content = await page.content();
  let ct: string | undefined = undefined;
  if (response) {
    headers = await response.allHeaders();
    ct = Object.entries(headers).find(([key]) => key.toLowerCase() === "content-type")?.[1];
    if (ct && (ct.toLowerCase().includes("application/json") || ct.toLowerCase().includes("text/plain"))) {
      content = (await response.body()).toString("utf8"); // TODO: determine real encoding
    }
  }

  return {
    content,
    status: response ? response.status() : null,
    headers,
    contentType: ct,
    screenshot: undefined as string | undefined,
  };
};

app.get('/health', async (req: Request, res: Response) => {
  try {
    if (!browser) {
      await initializeBrowser();
    }
    
    const testContext = await createContext();
    const testPage = await testContext.newPage();
    await testPage.close();
    await testContext.close();
    
    res.status(200).json({ 
      status: 'healthy',
      maxConcurrentPages: MAX_CONCURRENT_PAGES,
      activePages: MAX_CONCURRENT_PAGES - pageSemaphore.getAvailablePermits()
    });
  } catch (error) {
    console.error('Health check failed:', error);
    res.status(503).json({ 
      status: 'unhealthy', 
      error: error instanceof Error ? error.message : 'Unknown error occurred'
    });
  }
});

app.post('/scrape', async (req: Request, res: Response) => {
  const { url, wait_after_load = 0, timeout = 15000, headers, check_selector, skip_tls_verification = false, screenshot = false, screenshot_full_page = true }: UrlModel = req.body;

  console.log(`================= Scrape Request =================`);
  console.log(`URL: ${url}`);
  console.log(`Wait After Load: ${wait_after_load}`);
  console.log(`Timeout: ${timeout}`);
  console.log(`Headers: ${headers ? JSON.stringify(headers) : 'None'}`);
  console.log(`Check Selector: ${check_selector ? check_selector : 'None'}`);
  console.log(`Skip TLS Verification: ${skip_tls_verification}`);
  console.log(`Screenshot: ${screenshot}`);
  console.log(`==================================================`);

  if (!url) {
    return res.status(400).json({ error: 'URL is required' });
  }

  if (!isValidUrl(url)) {
    return res.status(400).json({ error: 'Invalid URL' });
  }

  if (!PROXY_SERVER) {
    console.warn('⚠️ WARNING: No proxy server provided. Your IP address may be blocked.');
  }

  if (!browser) {
    await initializeBrowser();
  }

  await pageSemaphore.acquire();
  
  let requestContext: BrowserContext | null = null;
  let page: Page | null = null;

  try {
    requestContext = await createContext(skip_tls_verification);
    page = await requestContext.newPage();

    if (headers) {
      await page.setExtraHTTPHeaders(headers);
    }

    // Route through Cloudflare proxy worker if configured
    const navigateUrl = CF_PROXY_URL ? `${CF_PROXY_URL.replace(/\/+$/, '')}/${url}` : url;
    if (CF_PROXY_URL) {
      console.log(`🔀 Routing through CF proxy: ${CF_PROXY_URL}`);
    }

    let result = await scrapePage(page, navigateUrl, 'load', wait_after_load, timeout, check_selector);

    // If CF proxy got a non-200 (e.g. DataDome 401, hard paywall), retry with
    // residential proxy + direct navigation. This lets Playwright handle JS
    // challenges (DataDome, etc.) with cookies on the real domain.
    if (result.status !== 200 && CF_PROXY_URL && RESIDENTIAL_PROXY_SERVER) {
      console.log(`🔄 CF proxy got ${result.status}, retrying via residential proxy...`);
      await page.close();
      await requestContext.close();

      requestContext = await createResidentialContext(skip_tls_verification);
      if (requestContext) {
        page = await requestContext.newPage();
        if (headers) {
          await page.setExtraHTTPHeaders(headers);
        }

        // Google News referrer unlocks paywalled sites that honour "first
        // click free" (WSJ, Bloomberg, FT, etc.).
        await page.setExtraHTTPHeaders({ 'Referer': 'https://news.google.com/' });

        // Navigate directly (no CF proxy) with networkidle to wait for
        // DataDome JS challenge to auto-resolve and page to reload.
        result = await scrapePage(page, url, 'networkidle', Math.max(wait_after_load, 3000), timeout, check_selector);

        if (result.status === 200) {
          console.log(`✅ Residential proxy bypass succeeded!`);
        } else {
          console.log(`🚨 Residential proxy also failed: ${result.status}`);
        }
      }
    }

    const pageError = result.status !== 200 ? getError(result.status) : undefined;

    // Capture screenshot if requested
    if (screenshot && page) {
      try {
        const screenshotBuffer = await page.screenshot({ fullPage: screenshot_full_page, type: 'png' });
        result.screenshot = `data:image/png;base64,${screenshotBuffer.toString('base64')}`;
        console.log(`📸 Screenshot captured (${Math.round(screenshotBuffer.length / 1024)}KB)`);
      } catch (screenshotError) {
        console.error('Screenshot capture failed:', screenshotError);
      }
    }

    if (!pageError) {
      console.log(`✅ Scrape successful!`);
    } else {
      console.log(`🚨 Scrape failed with status code: ${result.status} ${pageError}`);
    }

    res.json({
      content: result.content,
      pageStatusCode: result.status,
      contentType: result.contentType,
      ...(result.screenshot && { screenshot: result.screenshot }),
      ...(pageError && { pageError })
    });

  } catch (error) {
    console.error('Scrape error:', error);
    res.status(500).json({ error: 'An error occurred while fetching the page.' });
  } finally {
    if (page) await page.close();
    if (requestContext) await requestContext.close();
    pageSemaphore.release();
  }
});

app.listen(port, () => {
  initializeBrowser().then(() => {
    console.log(`Server is running on port ${port}`);
  });
});

if (require.main === module) {
  process.on('SIGINT', () => {
    shutdownBrowser().then(() => {
      console.log('Browser closed');
      process.exit(0);
    });
  });
}
