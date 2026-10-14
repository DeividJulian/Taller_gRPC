/**
 * Cola asíncrona: permite "empujar" eventos desde cualquier parte del servidor
 * y consumirlos con `for await`. Es el puente entre el Hub (que publica eventos)
 * y los generadores async que Connect usa para los streams de gRPC.
 */
export class AsyncQueue<T> implements AsyncIterable<T> {
  private items: T[] = [];
  private waiting: ((r: IteratorResult<T>) => void) | null = null;
  private closed = false;

  push(item: T): void {
    if (this.closed) return;
    if (this.waiting) {
      const resolve = this.waiting;
      this.waiting = null;
      resolve({ value: item, done: false });
    } else {
      this.items.push(item);
    }
  }

  /**
   * Para long-polling: espera hasta `ms` a que haya al menos un elemento y
   * devuelve todos los pendientes de una vez (puede ser una lista vacía).
   */
  async drain(ms: number, signal?: AbortSignal): Promise<T[]> {
    if (this.items.length === 0 && !this.closed && !this.waiting) {
      await new Promise<void>((resolve) => {
        const done = () => {
          clearTimeout(timer);
          signal?.removeEventListener("abort", done);
          if (this.waiting === onItem) this.waiting = null;
          resolve();
        };
        const onItem = (r: IteratorResult<T>) => {
          if (!r.done) this.items.unshift(r.value);
          done();
        };
        const timer = setTimeout(done, ms);
        signal?.addEventListener("abort", done);
        this.waiting = onItem;
      });
    }
    return this.items.splice(0, this.items.length);
  }

  close(): void {
    this.closed = true;
    if (this.waiting) {
      const resolve = this.waiting;
      this.waiting = null;
      resolve({ value: undefined as never, done: true });
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: () => {
        if (this.items.length > 0) {
          return Promise.resolve({ value: this.items.shift()!, done: false });
        }
        if (this.closed) {
          return Promise.resolve({ value: undefined as never, done: true });
        }
        return new Promise((resolve) => (this.waiting = resolve));
      },
      return: () => {
        this.close();
        return Promise.resolve({ value: undefined as never, done: true });
      },
      throw: (err?: unknown) => {
        this.close();
        return Promise.reject(err);
      },
    };
  }
}
