/** A suggestion read from the topic. */
export interface SuggestionMessage {
  email: string;
  suggestion: {
    name: string;
    edition: string;
    format: string;
    authors: string[];
  };
}

/** The email sent to the Emailer. */
export interface EmailRequest {
  email: string;
  subject: string;
  body: string;
}

const SUBJECT = "Don't forget to check this out!";

/** Builds the email for a suggestion. */
export function emailFor(message: SuggestionMessage): EmailRequest {
  const { name, edition, format, authors } = message.suggestion;
  const body = `We think you've recently considered ${name} (${surnames(authors)}). The ${edition} edition is available in ${format}!`;
  return { email: message.email, subject: SUBJECT, body };
}

/** The first word of each author's name, joined with commas: "Fowler Martin" becomes "Fowler". */
export function surnames(authors: string[]): string {
  return authors
    .map((author) => {
      const parts = author.trim().split(/\s+/);
      return parts.length > 0 ? parts[0] : '';
    })
    .join(', ');
}
