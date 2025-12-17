import fs from "node:fs/promises";
import path from "node:path";
import { z } from "zod";
import {
  agentRequestSchema,
  batchScrapeRequestSchema,
  crawlRequestSchema,
  extractRequestSchema,
  jobIdParamsSchema,
  mapRequestSchema,
  scrapeRequestSchema,
  searchRequestSchema,
} from "../controllers/v2/types";

type OpenAPIV3_1 = {
  openapi: "3.1.0";
  info: {
    title: string;
    version: string;
    description?: string;
  };
  servers: { url: string }[];
  tags?: { name: string; description?: string }[];
  paths: Record<string, any>;
  components: {
    securitySchemes?: Record<string, any>;
    schemas: Record<string, any>;
  };
};

function schemaRef(typeName: string) {
  return { $ref: `#/components/schemas/${typeName}` };
}

function zodToJsonSchema(
  schema: z.ZodTypeAny,
  io: "input" | "output" = "output",
) {
  // Zod v4 emits JSON Schema 2020-12, so we publish OpenAPI 3.1.0.
  // We keep $defs intact for recursive/union schemas.
  const jsonSchema = z.toJSONSchema(schema, {
    io,
    // v2 schemas use preprocess/transform heavily (URL normalization, etc).
    // Those can't be represented in JSON Schema, so we fall back to permissive
    // JSON Schema for the affected parts instead of throwing.
    unrepresentable: "any",
  });
  // OpenAPI component schemas don't need the $schema field.
  // Keeping it doesn't usually break anything, but removing reduces noise.
  const { $schema: _ignored, ...rest } = jsonSchema as any;
  return rest;
}

function zodObjectToParameters(
  objSchema: z.ZodTypeAny,
  location: "path" | "query",
) {
  const json = zodToJsonSchema(objSchema, "input");
  const props = (json as any)?.properties ?? {};
  const required = new Set<string>((json as any)?.required ?? []);

  return Object.entries<any>(props)
    .filter(([name]) => !name.startsWith("__"))
    .map(([name, schema]) => ({
      name,
      in: location,
      required: location === "path" ? true : required.has(name),
      schema,
    }));
}

async function main() {
  // This script is intended to be executed from apps/api (see package.json script).
  const apiRoot = path.resolve(process.cwd());

  const outPath = path.join(apiRoot, "openapi-v2.json");

  // Request body schemas come from the actual Zod validators used at runtime.
  // Response schemas are intentionally loose (many are TS-only types today).
  const ErrorResponseSchema = z.object({
    success: z.literal(false),
    code: z.string().optional(),
    error: z.string(),
    details: z.any().optional(),
  });

  const IdUrlSuccessSchema = z.object({
    success: z.literal(true),
    id: z.string(),
    url: z.string(),
  });

  const SimpleSuccessSchema = z.object({ success: z.boolean() });

  const schemas: Record<string, any> = {
    // Requests
    ScrapeRequest: zodToJsonSchema(scrapeRequestSchema, "input"),
    BatchScrapeRequest: zodToJsonSchema(batchScrapeRequestSchema, "input"),
    CrawlRequest: zodToJsonSchema(crawlRequestSchema, "input"),
    MapRequest: zodToJsonSchema(mapRequestSchema, "input"),
    SearchRequest: zodToJsonSchema(searchRequestSchema, "input"),
    ExtractRequest: zodToJsonSchema(extractRequestSchema, "input"),
    AgentRequest: zodToJsonSchema(agentRequestSchema, "input"),

    // Common / lightweight responses (best-effort)
    ErrorResponse: zodToJsonSchema(ErrorResponseSchema, "output"),
    CrawlResponse: zodToJsonSchema(
      z.union([ErrorResponseSchema, IdUrlSuccessSchema]),
      "output",
    ),
    BatchScrapeResponse: zodToJsonSchema(
      z.union([
        ErrorResponseSchema,
        IdUrlSuccessSchema.extend({
          invalidURLs: z.array(z.string()).optional(),
        }),
      ]),
      "output",
    ),
    AgentResponse: zodToJsonSchema(
      z.union([
        ErrorResponseSchema,
        z.object({ success: z.boolean(), id: z.string() }),
      ]),
      "output",
    ),
    AgentStatusResponse: zodToJsonSchema(
      z.union([
        ErrorResponseSchema,
        z.object({
          success: z.boolean(),
          status: z.enum(["processing", "completed", "failed"]),
          error: z.string().optional(),
          data: z.any().optional(),
          expiresAt: z.string(),
          creditsUsed: z.number().optional(),
        }),
      ]),
      "output",
    ),
    AgentCancelResponse: zodToJsonSchema(
      z.union([ErrorResponseSchema, z.object({ success: z.boolean() })]),
      "output",
    ),

    ScrapeResponse: zodToJsonSchema(SimpleSuccessSchema, "output"),
    CrawlStatusResponse: zodToJsonSchema(SimpleSuccessSchema, "output"),
    CrawlErrorsResponse: zodToJsonSchema(SimpleSuccessSchema, "output"),
    OngoingCrawlsResponse: zodToJsonSchema(SimpleSuccessSchema, "output"),
    MapResponse: zodToJsonSchema(SimpleSuccessSchema, "output"),
    SearchResponse: zodToJsonSchema(SimpleSuccessSchema, "output"),
    ExtractResponse: zodToJsonSchema(SimpleSuccessSchema, "output"),
  };

  const doc: OpenAPIV3_1 = {
    openapi: "3.1.0",
    info: {
      title: "Firecrawl API",
      version: "v2",
      description:
        "Autogenerated OpenAPI spec for the Firecrawl v2 API (derived from src/routes/v2.ts and src/controllers/v2/types.ts).",
    },
    servers: [{ url: "https://api.firecrawl.dev/v2" }],
    components: {
      securitySchemes: {
        bearerAuth: {
          type: "http",
          scheme: "bearer",
          bearerFormat: "API Key",
          description: "Provide your Firecrawl API key as a bearer token.",
        },
      },
      schemas,
    },
    paths: {
      "/scrape": {
        post: {
          tags: ["Scraping"],
          operationId: "v2Scrape",
          security: [{ bearerAuth: [] }],
          requestBody: {
            required: true,
            content: {
              "application/json": { schema: schemaRef("ScrapeRequest") },
            },
          },
          responses: {
            "200": {
              description: "Successful response",
              content: {
                "application/json": { schema: schemaRef("ScrapeResponse") },
              },
            },
          },
        },
      },
      "/scrape/{jobId}": {
        get: {
          tags: ["Scraping"],
          operationId: "v2ScrapeStatus",
          security: [{ bearerAuth: [] }],
          parameters: zodObjectToParameters(jobIdParamsSchema, "path"),
          responses: {
            "200": {
              description: "Successful response",
              content: {
                "application/json": { schema: schemaRef("ScrapeResponse") },
              },
            },
          },
        },
      },
      "/batch/scrape": {
        post: {
          tags: ["Scraping"],
          operationId: "v2BatchScrape",
          security: [{ bearerAuth: [] }],
          requestBody: {
            required: true,
            content: {
              "application/json": { schema: schemaRef("BatchScrapeRequest") },
            },
          },
          responses: {
            "200": {
              description: "Successful response",
              content: {
                "application/json": {
                  schema: schemaRef("BatchScrapeResponse"),
                },
              },
            },
          },
        },
      },
      "/batch/scrape/{jobId}": {
        get: {
          tags: ["Scraping"],
          operationId: "v2BatchScrapeStatus",
          security: [{ bearerAuth: [] }],
          parameters: zodObjectToParameters(jobIdParamsSchema, "path"),
          responses: {
            "200": {
              description: "Successful response",
              content: {
                "application/json": {
                  schema: schemaRef("CrawlStatusResponse"),
                },
              },
            },
          },
        },
        delete: {
          tags: ["Scraping"],
          operationId: "v2BatchScrapeCancel",
          security: [{ bearerAuth: [] }],
          parameters: zodObjectToParameters(jobIdParamsSchema, "path"),
          responses: {
            "200": {
              description: "Successful response",
              content: {
                "application/json": {
                  schema: schemaRef("AgentCancelResponse"),
                },
              },
            },
          },
        },
      },
      "/batch/scrape/{jobId}/errors": {
        get: {
          tags: ["Scraping"],
          operationId: "v2BatchScrapeErrors",
          security: [{ bearerAuth: [] }],
          parameters: zodObjectToParameters(jobIdParamsSchema, "path"),
          responses: {
            "200": {
              description: "Successful response",
              content: {
                "application/json": {
                  schema: schemaRef("CrawlErrorsResponse"),
                },
              },
            },
          },
        },
      },
      "/search": {
        post: {
          tags: ["Search"],
          operationId: "v2Search",
          security: [{ bearerAuth: [] }],
          requestBody: {
            required: true,
            content: {
              "application/json": { schema: schemaRef("SearchRequest") },
            },
          },
          responses: {
            "200": {
              description: "Successful response",
              content: {
                "application/json": { schema: schemaRef("SearchResponse") },
              },
            },
          },
        },
      },
      "/map": {
        post: {
          tags: ["Mapping"],
          operationId: "v2Map",
          security: [{ bearerAuth: [] }],
          requestBody: {
            required: true,
            content: {
              "application/json": { schema: schemaRef("MapRequest") },
            },
          },
          responses: {
            "200": {
              description: "Successful response",
              content: {
                "application/json": { schema: schemaRef("MapResponse") },
              },
            },
          },
        },
      },
      "/crawl": {
        post: {
          tags: ["Crawling"],
          operationId: "v2Crawl",
          security: [{ bearerAuth: [] }],
          requestBody: {
            required: true,
            content: {
              "application/json": { schema: schemaRef("CrawlRequest") },
            },
          },
          responses: {
            "200": {
              description: "Successful response",
              content: {
                "application/json": { schema: schemaRef("CrawlResponse") },
              },
            },
          },
        },
      },
      "/crawl/{jobId}": {
        get: {
          tags: ["Crawling"],
          operationId: "v2CrawlStatus",
          security: [{ bearerAuth: [] }],
          parameters: zodObjectToParameters(jobIdParamsSchema, "path"),
          responses: {
            "200": {
              description: "Successful response",
              content: {
                "application/json": {
                  schema: schemaRef("CrawlStatusResponse"),
                },
              },
            },
          },
        },
        delete: {
          tags: ["Crawling"],
          operationId: "v2CrawlCancel",
          security: [{ bearerAuth: [] }],
          parameters: zodObjectToParameters(jobIdParamsSchema, "path"),
          responses: {
            "200": {
              description: "Successful response",
              content: {
                "application/json": {
                  schema: schemaRef("AgentCancelResponse"),
                },
              },
            },
          },
        },
      },
      "/crawl/{jobId}/errors": {
        get: {
          tags: ["Crawling"],
          operationId: "v2CrawlErrors",
          security: [{ bearerAuth: [] }],
          parameters: zodObjectToParameters(jobIdParamsSchema, "path"),
          responses: {
            "200": {
              description: "Successful response",
              content: {
                "application/json": {
                  schema: schemaRef("CrawlErrorsResponse"),
                },
              },
            },
          },
        },
      },
      "/crawl/ongoing": {
        get: {
          tags: ["Crawling"],
          operationId: "v2CrawlOngoing",
          security: [{ bearerAuth: [] }],
          responses: {
            "200": {
              description: "Successful response",
              content: {
                "application/json": {
                  schema: schemaRef("OngoingCrawlsResponse"),
                },
              },
            },
          },
        },
      },
      "/crawl/active": {
        get: {
          tags: ["Crawling"],
          operationId: "v2CrawlActive",
          security: [{ bearerAuth: [] }],
          responses: {
            "200": {
              description: "Successful response",
              content: {
                "application/json": {
                  schema: schemaRef("OngoingCrawlsResponse"),
                },
              },
            },
          },
        },
      },
      "/extract": {
        post: {
          tags: ["Extract"],
          operationId: "v2Extract",
          security: [{ bearerAuth: [] }],
          requestBody: {
            required: true,
            content: {
              "application/json": { schema: schemaRef("ExtractRequest") },
            },
          },
          responses: {
            "200": {
              description: "Successful response",
              content: {
                "application/json": { schema: schemaRef("ExtractResponse") },
              },
            },
          },
        },
      },
      "/agent": {
        post: {
          tags: ["Agent"],
          operationId: "v2Agent",
          security: [{ bearerAuth: [] }],
          requestBody: {
            required: true,
            content: {
              "application/json": { schema: schemaRef("AgentRequest") },
            },
          },
          responses: {
            "200": {
              description: "Successful response",
              content: {
                "application/json": { schema: schemaRef("AgentResponse") },
              },
            },
          },
        },
      },
      "/agent/{jobId}": {
        get: {
          tags: ["Agent"],
          operationId: "v2AgentStatus",
          security: [{ bearerAuth: [] }],
          parameters: zodObjectToParameters(jobIdParamsSchema, "path"),
          responses: {
            "200": {
              description: "Successful response",
              content: {
                "application/json": {
                  schema: schemaRef("AgentStatusResponse"),
                },
              },
            },
          },
        },
        delete: {
          tags: ["Agent"],
          operationId: "v2AgentCancel",
          security: [{ bearerAuth: [] }],
          parameters: zodObjectToParameters(jobIdParamsSchema, "path"),
          responses: {
            "200": {
              description: "Successful response",
              content: {
                "application/json": {
                  schema: schemaRef("AgentCancelResponse"),
                },
              },
            },
          },
        },
      },
    },
  };

  await fs.writeFile(outPath, JSON.stringify(doc, null, 2) + "\n", "utf8");
}

main().catch(err => {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exitCode = 1;
});
