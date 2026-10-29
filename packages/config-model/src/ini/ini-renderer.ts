export type IniScalar = boolean | number | string;

export interface IniSection {
  readonly name: string;
  readonly values: Readonly<Record<string, IniScalar>>;
}

const INI_IDENTIFIER = /^[A-Za-z][A-Za-z0-9_]*$/u;

function renderValue(value: IniScalar): string {
  if (typeof value === 'number' && !Number.isFinite(value)) {
    throw new Error('INI_VALUE_INVALID');
  }
  const rendered = String(value);
  if (rendered.includes('\r') || rendered.includes('\n') || rendered.includes('\u0000')) {
    throw new Error('INI_VALUE_INVALID');
  }
  return rendered;
}

export function renderIni(sections: readonly IniSection[]): string {
  const sectionNames = new Set<string>();
  const output: string[] = [];
  for (const section of sections) {
    if (!INI_IDENTIFIER.test(section.name) || sectionNames.has(section.name)) {
      throw new Error('INI_SECTION_INVALID');
    }
    sectionNames.add(section.name);
    output.push(`[${section.name}]`);
    for (const [key, value] of Object.entries(section.values)) {
      if (!INI_IDENTIFIER.test(key)) {
        throw new Error('INI_KEY_INVALID');
      }
      output.push(`${key}=${renderValue(value)}`);
    }
    output.push('');
  }
  return `${output.join('\n')}\n`;
}
