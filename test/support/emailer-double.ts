import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';

/** The Emailer's base URL as the application sees it. MSW intercepts it; nothing listens there. */
export const EMAILER_BASE_URL = 'http://localhost:9001';

/** The Emailer's path. The recipient's address is in the JSON body, not the path. */
export const EMAIL_PATH = '/Emailer/email';

export interface ReceivedRequest {
  method: string;
  path: string;
  contentType: string | null;
  accept: string | null;
  body: unknown;
}

/**
 * A test double for the Emailer. It answers
 * POST /Emailer/email with 204 and records each request, for the test to check.
 */
export class EmailerDouble {
  readonly server = setupServer();
  private readonly received: ReceivedRequest[] = [];

  async start(): Promise<void> {
    this.server.use(
      http.post(`${EMAILER_BASE_URL}${EMAIL_PATH}`, async ({ request }) => {
        const text = await request.text();
        this.received.push({
          method: request.method,
          path: new URL(request.url).pathname,
          contentType: request.headers.get('content-type'),
          accept: request.headers.get('accept'),
          body: text ? JSON.parse(text) : undefined,
        });
        return new HttpResponse(null, { status: 204 });
      }),
    );
    this.server.listen({ onUnhandledFrame: 'error' });
  }

  requests(): ReadonlyArray<ReceivedRequest> {
    return this.received;
  }

  async stop(): Promise<void> {
    this.server.close();
  }
}
