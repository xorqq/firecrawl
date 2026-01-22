/**
 * Utility to convert Zod schemas to JSON Schema with support for both Zod v3 and v4.
 *
 * - Zod v4+: Uses built-in `z.toJSONSchema()` function (if available via the schema)
 * - Zod v3: Falls back to `zod-to-json-schema` package
 *
 * This provides forward compatibility for users upgrading to Zod v4 while
 * maintaining backwards compatibility with Zod v3.
 */

import { zodToJsonSchema as zodToJsonSchemaLib } from "zod-to-json-schema";

type SchemaConverter = (schema: unknown) => unknown;

/**
 * Detects if a value is a Zod schema (v3 or v4).
 */
export function isZodSchema(value: unknown): boolean {
  if (!value || typeof value !== "object") return false;
  const schema = value as Record<string, unknown>;

  // Check for Zod v3 characteristics
  const hasV3Markers =
    "_def" in schema &&
    (typeof schema.safeParse === "function" ||
      typeof schema.parse === "function");

  // Check for Zod v4 characteristics (v4 schemas have _zod property)
  const hasV4Markers = "_zod" in schema && typeof schema._zod === "object";

  return hasV3Markers || hasV4Markers;
}

/**
 * Detects if a Zod schema is from Zod v4 (has _zod property).
 */
function isZodV4Schema(schema: unknown): boolean {
  if (!schema || typeof schema !== "object") return false;
  return "_zod" in schema && typeof (schema as Record<string, unknown>)._zod === "object";
}

/**
 * Tries to convert a Zod v4 schema using the built-in toJSONSchema method.
 * Returns null if not a v4 schema or if conversion fails.
 */
function tryZodV4Conversion(schema: unknown): Record<string, unknown> | null {
  if (!isZodV4Schema(schema)) return null;

  try {
    // In Zod v4, the toJSONSchema function is available on the z namespace
    // We can access it through the schema's constructor or the global z object
    // For now, we'll try to access it through the module
    const zodModule = (schema as Record<string, unknown>).constructor?.prototype?.constructor;
    if (zodModule && typeof (zodModule as Record<string, unknown>).toJSONSchema === "function") {
      return (zodModule as { toJSONSchema: SchemaConverter }).toJSONSchema(schema) as Record<string, unknown>;
    }
  } catch {
    // V4 conversion not available
  }

  return null;
}

/**
 * Converts a Zod schema to JSON Schema.
 *
 * Automatically detects whether to use Zod v4's built-in `z.toJSONSchema()`
 * or falls back to `zod-to-json-schema` for Zod v3.
 *
 * @param schema - A Zod schema to convert
 * @returns The JSON Schema representation
 * @throws Error if conversion fails
 */
export function zodSchemaToJsonSchema(schema: unknown): Record<string, unknown> {
  if (!isZodSchema(schema)) {
    throw new Error("Provided value is not a Zod schema");
  }

  // Try Zod v4's built-in toJSONSchema first (if available)
  const v4Result = tryZodV4Conversion(schema);
  if (v4Result) {
    return v4Result;
  }

  // Fall back to zod-to-json-schema for Zod v3
  try {
    return zodToJsonSchemaLib(schema as Parameters<typeof zodToJsonSchemaLib>[0]) as Record<string, unknown>;
  } catch (err) {
    throw new Error(
      `Failed to convert Zod schema to JSON Schema: ${err instanceof Error ? err.message : String(err)}`
    );
  }
}

/**
 * Safely converts a potential Zod schema to JSON Schema.
 * Returns the original value if it's not a Zod schema or if conversion fails.
 *
 * @param schema - A potential Zod schema or existing JSON schema
 * @returns The JSON Schema representation, or the original value if not a Zod schema
 */
export function safeZodSchemaToJsonSchema(
  schema: unknown
): Record<string, unknown> | unknown {
  if (!isZodSchema(schema)) {
    return schema;
  }

  try {
    return zodSchemaToJsonSchema(schema);
  } catch {
    // If conversion fails, return original value
    // Server-side may still handle it, or request will fail explicitly
    return schema;
  }
}

/**
 * Detects if an object looks like a Zod schema's `.shape` property.
 * When users mistakenly pass `schema.shape` instead of `schema`, the object
 * will have Zod types as values but won't be a Zod schema itself.
 */
export function looksLikeZodShape(obj: unknown): boolean {
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) return false;
  const values = Object.values(obj);
  if (values.length === 0) return false;
  // Check if at least one value looks like a Zod type
  return values.some(
    (v) =>
      v &&
      typeof v === "object" &&
      (v as Record<string, unknown>)._def &&
      typeof (v as Record<string, unknown>).safeParse === "function"
  );
}
