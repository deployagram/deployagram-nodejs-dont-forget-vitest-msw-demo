import type { EmailRequest } from './suggestion-email.js';

export const EMAIL_PATH = '/Emailer/email';

/** Thrown when the Emailer does not accept an email. */
export class EmailerError extends Error {
  readonly status: number;

  constructor(status: number) {
    super(`Emailer responded with HTTP ${status}`);
    this.name = 'EmailerError';
    this.status = status;
  }
}

/** POSTs the email, in the request body, to the Emailer. */
export async function sendEmail(baseUrl: string, email: EmailRequest): Promise<void> {
  const response = await fetch(baseUrl.replace(/\/+$/, '') + EMAIL_PATH, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(email),
  });
  if (!response.ok) {
    throw new EmailerError(response.status);
  }
}
