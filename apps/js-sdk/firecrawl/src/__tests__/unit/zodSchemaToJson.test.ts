import { describe, test, expect } from "@jest/globals";
import { z } from "zod";
import {
  isZodSchema,
  zodSchemaToJsonSchema,
  safeZodSchemaToJsonSchema,
  looksLikeZodShape,
} from "../../utils/zodSchemaToJson";

describe("zodSchemaToJson utility", () => {
  describe("isZodSchema", () => {
    test("returns true for Zod v3 object schema", () => {
      const schema = z.object({ name: z.string() });
      expect(isZodSchema(schema)).toBe(true);
    });

    test("returns true for Zod v3 string schema", () => {
      const schema = z.string();
      expect(isZodSchema(schema)).toBe(true);
    });

    test("returns true for Zod v3 number schema", () => {
      const schema = z.number();
      expect(isZodSchema(schema)).toBe(true);
    });

    test("returns true for Zod v3 array schema", () => {
      const schema = z.array(z.string());
      expect(isZodSchema(schema)).toBe(true);
    });

    test("returns true for Zod v3 enum schema", () => {
      const schema = z.enum(["A", "B", "C"]);
      expect(isZodSchema(schema)).toBe(true);
    });

    test("returns true for Zod v3 union schema", () => {
      const schema = z.union([z.string(), z.number()]);
      expect(isZodSchema(schema)).toBe(true);
    });

    test("returns true for Zod v3 optional schema", () => {
      const schema = z.string().optional();
      expect(isZodSchema(schema)).toBe(true);
    });

    test("returns true for Zod v3 nullable schema", () => {
      const schema = z.string().nullable();
      expect(isZodSchema(schema)).toBe(true);
    });

    test("returns false for null", () => {
      expect(isZodSchema(null)).toBe(false);
    });

    test("returns false for undefined", () => {
      expect(isZodSchema(undefined)).toBe(false);
    });

    test("returns false for plain objects", () => {
      expect(isZodSchema({ name: "test" })).toBe(false);
    });

    test("returns false for plain JSON schema objects", () => {
      const jsonSchema = {
        type: "object",
        properties: {
          name: { type: "string" },
        },
        required: ["name"],
      };
      expect(isZodSchema(jsonSchema)).toBe(false);
    });

    test("returns false for strings", () => {
      expect(isZodSchema("test")).toBe(false);
    });

    test("returns false for numbers", () => {
      expect(isZodSchema(42)).toBe(false);
    });

    test("returns false for arrays", () => {
      expect(isZodSchema([1, 2, 3])).toBe(false);
    });
  });

  describe("zodSchemaToJsonSchema", () => {
    test("converts simple object schema", () => {
      const schema = z.object({ name: z.string() });
      const result = zodSchemaToJsonSchema(schema);

      expect(result).toBeDefined();
      expect(result.type).toBe("object");
      expect(result.properties).toBeDefined();
      expect((result.properties as Record<string, unknown>).name).toBeDefined();
    });

    test("converts schema with multiple fields", () => {
      const schema = z.object({
        name: z.string(),
        age: z.number(),
        email: z.string().email(),
      });
      const result = zodSchemaToJsonSchema(schema);

      expect(result.type).toBe("object");
      expect(result.properties).toBeDefined();
      const props = result.properties as Record<string, unknown>;
      expect(props.name).toBeDefined();
      expect(props.age).toBeDefined();
      expect(props.email).toBeDefined();
    });

    test("converts nested object schema", () => {
      const schema = z.object({
        user: z.object({
          name: z.string(),
          address: z.object({
            city: z.string(),
            zip: z.string(),
          }),
        }),
      });
      const result = zodSchemaToJsonSchema(schema);

      expect(result.type).toBe("object");
      expect(result.properties).toBeDefined();
    });

    test("converts array schema", () => {
      const schema = z.array(z.string());
      const result = zodSchemaToJsonSchema(schema);

      expect(result.type).toBe("array");
      expect(result.items).toBeDefined();
    });

    test("converts enum schema", () => {
      const schema = z.enum(["apple", "banana", "cherry"]);
      const result = zodSchemaToJsonSchema(schema);

      expect(result.type).toBe("string");
      expect(result.enum).toEqual(["apple", "banana", "cherry"]);
    });

    test("converts union schema", () => {
      const schema = z.union([z.string(), z.number()]);
      const result = zodSchemaToJsonSchema(schema);

      // Union schemas in JSON Schema can use type array, anyOf, or oneOf
      // zod-to-json-schema uses type array for simple unions
      expect(result.type || result.anyOf || result.oneOf).toBeDefined();
    });

    test("converts optional fields", () => {
      const schema = z.object({
        required: z.string(),
        optional: z.string().optional(),
      });
      const result = zodSchemaToJsonSchema(schema);

      expect(result.type).toBe("object");
      // The 'required' array should only contain 'required', not 'optional'
      expect(result.required).toEqual(["required"]);
    });

    test("converts nullable fields", () => {
      const schema = z.object({
        name: z.string().nullable(),
      });
      const result = zodSchemaToJsonSchema(schema);

      expect(result.type).toBe("object");
      expect(result.properties).toBeDefined();
    });

    test("converts schema with default values", () => {
      const schema = z.object({
        name: z.string().default("Unknown"),
        count: z.number().default(0),
      });
      const result = zodSchemaToJsonSchema(schema);

      expect(result.type).toBe("object");
      expect(result.properties).toBeDefined();
    });

    test("throws error for non-Zod values", () => {
      expect(() => zodSchemaToJsonSchema({ name: "test" })).toThrow(
        "Provided value is not a Zod schema"
      );
    });

    test("throws error for null", () => {
      expect(() => zodSchemaToJsonSchema(null)).toThrow(
        "Provided value is not a Zod schema"
      );
    });

    test("throws error for undefined", () => {
      expect(() => zodSchemaToJsonSchema(undefined)).toThrow(
        "Provided value is not a Zod schema"
      );
    });
  });

  describe("safeZodSchemaToJsonSchema", () => {
    test("converts Zod schema to JSON schema", () => {
      const schema = z.object({ title: z.string() });
      const result = safeZodSchemaToJsonSchema(schema);

      expect(result).toBeDefined();
      expect((result as Record<string, unknown>).type).toBe("object");
      expect((result as Record<string, unknown>).properties).toBeDefined();
    });

    test("returns original value for plain JSON schema objects", () => {
      const jsonSchema = {
        type: "object",
        properties: { name: { type: "string" } },
        required: ["name"],
      };
      const result = safeZodSchemaToJsonSchema(jsonSchema);

      expect(result).toEqual(jsonSchema);
    });

    test("returns original value for null", () => {
      expect(safeZodSchemaToJsonSchema(null)).toBe(null);
    });

    test("returns original value for undefined", () => {
      expect(safeZodSchemaToJsonSchema(undefined)).toBe(undefined);
    });

    test("returns original value for plain objects", () => {
      const obj = { name: "test" };
      expect(safeZodSchemaToJsonSchema(obj)).toEqual(obj);
    });

    test("returns original value for strings", () => {
      expect(safeZodSchemaToJsonSchema("test")).toBe("test");
    });

    test("returns original value for numbers", () => {
      expect(safeZodSchemaToJsonSchema(42)).toBe(42);
    });

    test("handles complex schemas without throwing", () => {
      const schema = z.object({
        id: z.string().uuid(),
        timestamp: z.string().datetime(),
        metadata: z.record(z.unknown()),
      });

      // Should not throw
      const result = safeZodSchemaToJsonSchema(schema);
      expect(result).toBeDefined();
      expect((result as Record<string, unknown>).type).toBe("object");
    });
  });

  describe("looksLikeZodShape", () => {
    test("returns true for Zod schema shape", () => {
      const schema = z.object({ title: z.string(), count: z.number() });
      expect(looksLikeZodShape(schema.shape)).toBe(true);
    });

    test("returns false for complete Zod schema", () => {
      const schema = z.object({ title: z.string() });
      // Complete Zod schema is not the same as .shape
      expect(looksLikeZodShape(schema)).toBe(false);
    });

    test("returns false for null", () => {
      expect(looksLikeZodShape(null)).toBe(false);
    });

    test("returns false for undefined", () => {
      expect(looksLikeZodShape(undefined)).toBe(false);
    });

    test("returns false for plain objects", () => {
      expect(looksLikeZodShape({ name: "test" })).toBe(false);
    });

    test("returns false for empty objects", () => {
      expect(looksLikeZodShape({})).toBe(false);
    });

    test("returns false for arrays", () => {
      expect(looksLikeZodShape([1, 2, 3])).toBe(false);
    });

    test("returns false for JSON schema objects", () => {
      const jsonSchema = {
        type: "object",
        properties: { name: { type: "string" } },
      };
      expect(looksLikeZodShape(jsonSchema)).toBe(false);
    });
  });

  describe("integration tests", () => {
    test("SDK-like usage: extract schema conversion", () => {
      const userSchema = z.object({
        name: z.string(),
        email: z.string().email(),
        age: z.number().min(0).max(150),
      });

      // Simulate what the SDK does
      if (isZodSchema(userSchema)) {
        const jsonSchema = safeZodSchemaToJsonSchema(userSchema);
        expect(jsonSchema).toBeDefined();
        expect((jsonSchema as Record<string, unknown>).type).toBe("object");
      }
    });

    test("SDK-like usage: handles pre-existing JSON schema", () => {
      const preExistingJsonSchema = {
        type: "object" as const,
        properties: {
          title: { type: "string" as const },
          description: { type: "string" as const },
        },
        required: ["title"] as string[],
      };

      // Simulate what the SDK does - should return original JSON schema
      if (isZodSchema(preExistingJsonSchema)) {
        // This branch should not execute
        expect(true).toBe(false);
      } else {
        const result = safeZodSchemaToJsonSchema(preExistingJsonSchema);
        expect(result).toEqual(preExistingJsonSchema);
      }
    });

    test("SDK-like usage: complex nested schema", () => {
      const productSchema = z.object({
        id: z.string().uuid(),
        name: z.string().min(1).max(100),
        price: z.number().positive(),
        categories: z.array(z.string()),
        metadata: z.object({
          createdAt: z.string(),
          updatedAt: z.string(),
          tags: z.array(z.string()).optional(),
        }),
        variants: z.array(
          z.object({
            sku: z.string(),
            color: z.string().optional(),
            size: z.string().optional(),
          })
        ),
      });

      const result = safeZodSchemaToJsonSchema(productSchema);
      expect(result).toBeDefined();
      expect((result as Record<string, unknown>).type).toBe("object");
      expect((result as Record<string, unknown>).properties).toBeDefined();
    });

    test("SDK-like usage: detects .shape mistake and provides useful error info", () => {
      const schema = z.object({ title: z.string() });

      // User mistakenly passes schema.shape
      if (looksLikeZodShape(schema.shape)) {
        // SDK would throw an error here with helpful message
        expect(true).toBe(true);
      } else {
        expect(true).toBe(false);
      }
    });
  });
});
