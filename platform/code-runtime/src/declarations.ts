import type { Json, JsonSchema, ToolManifestEntry } from "./contract.js";

export interface DeclarationOptions {
  /** Schema of the script's `input`; `unknown` when omitted. */
  inputSchema?: JsonSchema;
}

/**
 * TypeScript declarations for everything a script can reach, generated from
 * the same manifest the runtime enforces. Show these to whoever writes the
 * script; the runtime itself never type-checks against them.
 */
export function generateDeclarations(
  tools: Pick<ToolManifestEntry, "name" | "description" | "input_schema" | "output_schema">[],
  options: DeclarationOptions = {},
): string {
  const root: Namespace = { members: new Map() };
  for (const tool of tools) {
    const parts = tool.name.split(".");
    let node = root;
    for (const part of parts.slice(0, -1)) {
      let child = node.members.get(part);
      if (!child || "tool" in child) {
        child = { members: new Map() };
        node.members.set(part, child);
      }
      node = child;
    }
    node.members.set(parts[parts.length - 1] ?? tool.name, { tool });
  }

  const input = options.inputSchema === undefined ? "unknown" : typeOf(options.inputSchema);
  return [
    `declare const input: ${input};`,
    "",
    "/** Durable pause. Timers such as setTimeout are not available. */",
    "declare function sleep(ms: number): Promise<void>;",
    "",
    "/** Thrown by a tool call that is rejected or fails. */",
    "declare class ToolError extends Error {",
    "  tool: string;",
    "  code: string;",
    "  details: unknown;",
    "}",
    "",
    `declare const tools: ${renderNamespace(root, "")};`,
    "",
  ].join("\n");
}

interface Namespace {
  members: Map<string, Namespace | { tool: DeclaredTool }>;
}

type DeclaredTool = Pick<
  ToolManifestEntry,
  "name" | "description" | "input_schema" | "output_schema"
>;

function renderNamespace(node: Namespace, indent: string): string {
  const inner = `${indent}  `;
  const lines: string[] = ["{"];
  for (const [name, member] of node.members) {
    if ("tool" in member) {
      const { tool } = member;
      if (tool.description) lines.push(...comment(tool.description, inner));
      const args = typeOf(tool.input_schema, inner);
      const result = tool.output_schema === undefined ? "unknown" : typeOf(tool.output_schema, inner);
      lines.push(`${inner}${name}(args: ${args}): Promise<${result}>;`);
    } else {
      lines.push(`${inner}${name}: ${renderNamespace(member, inner)};`);
    }
  }
  lines.push(`${indent}}`);
  return lines.join("\n");
}

function comment(text: string, indent: string): string[] {
  const safe = text.replaceAll("*/", "*\\/").split("\n");
  if (safe.length === 1) return [`${indent}/** ${safe[0]} */`];
  return [`${indent}/**`, ...safe.map((line) => `${indent} * ${line}`), `${indent} */`];
}

type SchemaObject = { [key: string]: Json };

function typeOf(schema: JsonSchema | Json | undefined, indent = "", root?: SchemaObject, depth = 0): string {
  if (schema === true || schema === undefined) return "unknown";
  if (schema === false) return "never";
  if (typeof schema !== "object" || schema === null || Array.isArray(schema)) return "unknown";
  const base = root ?? schema;
  const recurse = (child: Json | undefined, childIndent = indent) =>
    typeOf(child, childIndent, base, depth + 1);

  if (typeof schema.$ref === "string") {
    // Recursive schemas cannot be inlined; past a fixed depth they widen.
    const target = depth > 8 ? undefined : resolve(base, schema.$ref);
    return target === undefined ? "unknown" : recurse(target);
  }
  if ("const" in schema) return JSON.stringify(schema.const);
  if (Array.isArray(schema.enum)) {
    return schema.enum.map((value) => JSON.stringify(value)).join(" | ") || "never";
  }
  for (const keyword of ["anyOf", "oneOf"] as const) {
    const options = schema[keyword];
    if (Array.isArray(options)) return union(options.map((option) => recurse(option)));
  }
  if (Array.isArray(schema.allOf)) {
    return schema.allOf.map((part) => `(${recurse(part)})`).join(" & ") || "unknown";
  }
  if (Array.isArray(schema.type)) {
    return union(schema.type.map((type) => recurse({ ...schema, type })));
  }

  switch (schema.type) {
    case "string":
      return "string";
    case "number":
    case "integer":
      return "number";
    case "boolean":
      return "boolean";
    case "null":
      return "null";
    case "array": {
      const item = recurse(schema.items);
      return /^[\w.]+$/.test(item) ? `${item}[]` : `Array<${item}>`;
    }
    case "object":
      return objectType(schema, indent, recurse);
    default:
      return "properties" in schema ? objectType(schema, indent, recurse) : "unknown";
  }
}

function objectType(
  schema: SchemaObject,
  indent: string,
  recurse: (child: Json | undefined, childIndent?: string) => string,
): string {
  const properties =
    typeof schema.properties === "object" && schema.properties !== null && !Array.isArray(schema.properties)
      ? schema.properties
      : {};
  const required = new Set(Array.isArray(schema.required) ? schema.required : []);
  const inner = `${indent}  `;
  const lines: string[] = [];
  for (const [name, property] of Object.entries(properties)) {
    const description =
      typeof property === "object" && property !== null && !Array.isArray(property)
        ? property.description
        : undefined;
    if (typeof description === "string") lines.push(...comment(description, inner));
    const key = /^[A-Za-z_$][\w$]*$/.test(name) ? name : JSON.stringify(name);
    lines.push(`${inner}${key}${required.has(name) ? "" : "?"}: ${recurse(property, inner)};`);
  }
  const extra = schema.additionalProperties;
  if (typeof extra === "object" && extra !== null) {
    lines.push(`${inner}[key: string]: ${recurse(extra, inner)};`);
  }
  if (lines.length === 0) {
    return extra === false ? "{}" : "{ [key: string]: unknown }";
  }
  return `{\n${lines.join("\n")}\n${indent}}`;
}

function union(types: string[]): string {
  const unique = [...new Set(types)];
  if (unique.includes("unknown")) return "unknown";
  return unique.join(" | ") || "never";
}

function resolve(root: SchemaObject, ref: string): Json | undefined {
  if (!ref.startsWith("#/")) return undefined;
  let node: Json | undefined = root;
  for (const raw of ref.slice(2).split("/")) {
    const key = raw.replaceAll("~1", "/").replaceAll("~0", "~");
    if (typeof node !== "object" || node === null || Array.isArray(node)) return undefined;
    node = node[key];
  }
  return node;
}
