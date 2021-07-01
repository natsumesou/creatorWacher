export const sleep = (msec: number) => new Promise((resolve) => setTimeout(resolve, msec));

export const concurrentPromise = async (promises: (() => Promise<any>)[], concurrency: number, sleepMillisec: number) => {
  const results: any[] = [];
  let currentIndex = 0;

  for (;;) {
    const chunks = promises.slice(currentIndex, currentIndex + concurrency);
    if (chunks.length === 0) {
      break;
    }
    Array.prototype.push.apply(results, await Promise.all(chunks.map((c) => c())));
    currentIndex += concurrency;
    sleep(sleepMillisec);
  }
  return results;
};
