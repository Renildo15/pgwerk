// The TypeScript client under test for conformance/client (see conformance/README.md).
//   URL=… TOKEN=… node clients/ts/conformance/worker.ts
import { RemoteWorker } from "../src/index.ts";

declare const process: { env: Record<string, string | undefined> };

const worker = new RemoteWorker({
  url: process.env.URL!,
  token: process.env.TOKEN!,
  queues: ["conformance"],
  insecure: true,
  log: () => {},
});

worker
  .handle("conformance.echo", (_ctx, kwargs) => kwargs)
  .handle("conformance.sleep", (ctx, { seconds }) =>
    new Promise<null>((resolve) => {
      const timer = setTimeout(() => resolve(null), Number(seconds) * 1000);
      ctx.signal.addEventListener("abort", () => (clearTimeout(timer), resolve(null)), { once: true });
    }),
  )
  .handle("conformance.fail", (_ctx, { message }) => {
    throw new Error(String(message));
  })
  .handle("conformance.big", (_ctx, { size }) => "x".repeat(Number(size)));

await worker.run();
