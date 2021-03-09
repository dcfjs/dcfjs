export class IncompleteBufferError extends Error {
  constructor(message?: string) {
    super();
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, this.constructor); // super helper method to include stack trace in error object
    }
    this.name = this.constructor.name;
    this.message = message || 'unable to decode';
  }
}

export function isFloat(n: number) {
  return n % 1 !== 0;
}
