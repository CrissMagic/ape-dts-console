const SAFE_IDENTIFIER = /^[a-z][a-z0-9_]*$/u;

export function quoteDatabaseIdentifier(identifier: string): string {
  if (!SAFE_IDENTIFIER.test(identifier)) {
    throw new Error(`unsafe PostgreSQL identifier: ${identifier}`);
  }
  return `"${identifier}"`;
}

export function qualifiedTable(schema: string, table: string): string {
  return `${quoteDatabaseIdentifier(schema)}.${quoteDatabaseIdentifier(table)}`;
}
