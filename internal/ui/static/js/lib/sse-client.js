/**
 * SSE client wrapper for NalaDB watch subscriptions.
 */
export class SSEClient {
  constructor() {
    this._source = null;
    this._onEvent = null;
    this._onError = null;
    this._onOpen = null;
  }

  get connected() {
    return this._source !== null && this._source.readyState !== EventSource.CLOSED;
  }

  /**
   * Subscribe to key changes.
   * @param {string[]} keys - Keys to watch.
   * @param {object} callbacks - { onEvent, onError, onOpen }
   */
  subscribe(keys, { onEvent, onError, onOpen } = {}) {
    this.disconnect();

    const params = new URLSearchParams({ keys: keys.join(',') });
    this._source = new EventSource(`/api/watch?${params}`);
    this._onEvent = onEvent;
    this._onError = onError;
    this._onOpen = onOpen;

    this._source.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data);
        if (data.error) {
          this._onError?.(data.error);
          return;
        }
        this._onEvent?.(data);
      } catch (err) {
        this._onError?.(err.message);
      }
    };

    this._source.onerror = () => {
      this._onError?.('Connection lost');
    };

    this._source.onopen = () => {
      this._onOpen?.();
    };
  }

  disconnect() {
    if (this._source) {
      this._source.close();
      this._source = null;
    }
  }
}
