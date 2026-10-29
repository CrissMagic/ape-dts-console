export class RuntimeText {
  readonly #previewValue: string;
  readonly #runtimeValue: string;

  constructor(runtimeValue: string, previewValue: string) {
    if (runtimeValue.length === 0 || previewValue.length === 0) {
      throw new Error('RUNTIME_TEXT_EMPTY');
    }
    this.#runtimeValue = runtimeValue;
    this.#previewValue = previewValue;
    Object.freeze(this);
  }

  preview(): string {
    return this.#previewValue;
  }

  reveal(): string {
    return this.#runtimeValue;
  }

  toJSON(): string {
    return this.#previewValue;
  }
}

export function runtimeText(runtimeValue: string, previewValue: string): RuntimeText {
  return new RuntimeText(runtimeValue, previewValue);
}
