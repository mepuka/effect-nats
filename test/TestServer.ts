import { Data, Effect } from "effect"
import type { Scope } from "effect"
import { execFile, spawn } from "node:child_process"
import type { ChildProcess } from "node:child_process"
import { closeSync, mkdtempSync, openSync, rmSync } from "node:fs"
import { createServer } from "node:net"
import { tmpdir } from "node:os"
import { join } from "node:path"

/**
 * Scoped local `nats-server -js` for integration tests. Acquire spawns the
 * server on a free port with a throwaway store dir; release kills the process
 * and removes the dir. Readiness is proven by a real TCP accept, not a sleep.
 */
export class TestServerError extends Data.TaggedError("TestServerError")<{
  readonly reason: string
}> {}

export interface TestServer {
  readonly port: number
  readonly url: string
  /** Simulates a server loss while the owning test scope remains open. */
  readonly stop: Effect.Effect<void>
}

export interface TestServerOptions {
  readonly jetStream?: boolean | undefined
  readonly name?: string | undefined
  readonly tls?: {
    readonly certFile: string
    readonly keyFile: string
  } | undefined
}

export interface IntegrationReceipt {
  readonly serverVersion: string
  readonly jetStreamAndStorage: string
  readonly clientRuntime: string
  readonly clientTransport: string
  readonly isolation: string
}

const requiredServerVersion = "v2.14.5"

const installInstruction =
  "Install v2.14.5 from https://github.com/nats-io/nats-server/releases/tag/v2.14.5 and add nats-server to PATH."

const runVersionProbe = (binary: string): Promise<IntegrationReceipt> =>
  new Promise((resolve, reject) => {
    execFile(binary, ["--version"], { windowsHide: true }, (error, stdout, stderr) => {
      if (error !== null) {
        const code = (error as NodeJS.ErrnoException).code
        const reason = code === "ENOENT"
          ? `nats-server ${requiredServerVersion} is required for live tests, but "${binary}" was not found on PATH. ${installInstruction}`
          : `nats-server ${requiredServerVersion} is required for live tests, but "${binary} --version" failed: ${error.message}. ${installInstruction}`
        reject(new TestServerError({ reason }))
        return
      }
      const output = `${stdout}\n${stderr}`.trim()
      const serverVersion = output.match(/\bv\d+\.\d+\.\d+\b/)?.[0]
      if (serverVersion !== requiredServerVersion) {
        reject(
          new TestServerError({
            reason:
              `nats-server ${requiredServerVersion} is required for live tests, but "${binary} --version" reported ${
                JSON.stringify(output)
              }. ${installInstruction}`
          })
        )
        return
      }
      resolve({
        serverVersion,
        jetStreamAndStorage:
          "per TestServer.startWith: default -js with per-scope OS-temp storage; Core fixtures may opt out",
        clientRuntime: `Node ${process.version} on ${process.platform}/${process.arch}`,
        clientTransport: "@nats-io/transport-node 3.4.0",
        isolation:
          "per-scope free port and OS-temp effect-nats-js-* store, removed by the owning Scope"
      })
    })
  })

export const probeNatsServer = (
  binary = "nats-server"
): Effect.Effect<IntegrationReceipt, TestServerError> =>
  Effect.tryPromise({
    try: () => runVersionProbe(binary),
    catch: (cause) =>
      cause instanceof TestServerError
        ? cause
        : new TestServerError({ reason: `nats-server preflight failed: ${String(cause)}` })
  })

let sharedPreflight: Promise<IntegrationReceipt> | undefined
let receiptPrinted = false

const claimRunReceipt = (): boolean => {
  const marker = join(tmpdir(), `effect-nats-integration-receipt-${process.ppid}.marker`)
  try {
    closeSync(openSync(marker, "wx"))
    return true
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code === "EEXIST") return false
    return true
  }
}

const preflightOnce = Effect.tryPromise({
  try: () => sharedPreflight ??= runVersionProbe("nats-server"),
  catch: (cause) =>
    cause instanceof TestServerError
      ? cause
      : new TestServerError({ reason: `nats-server preflight failed: ${String(cause)}` })
}).pipe(
  Effect.tap((receipt) =>
    Effect.sync(() => {
      if (receiptPrinted || !claimRunReceipt()) return
      receiptPrinted = true
      process.stdout.write(
        `[effect-nats live integration] server=${receipt.serverVersion}; JetStream/storage=${receipt.jetStreamAndStorage}; runtime=${receipt.clientRuntime}; transport=${receipt.clientTransport}; isolation=${receipt.isolation}\n`
      )
    })
  )
)

const freePort = Effect.callback<number>((resume) => {
  const probe = createServer()
  probe.listen(0, "127.0.0.1", () => {
    const address = probe.address()
    const port = typeof address === "object" && address !== null ? address.port : 0
    probe.close(() => resume(Effect.succeed(port)))
  })
})

const waitReady = (child: ChildProcess): Effect.Effect<void, TestServerError> =>
  Effect.callback<void, TestServerError>((resume) => {
    let settled = false
    const done = (result: Effect.Effect<void, TestServerError>) => {
      if (settled) return
      settled = true
      child.stderr?.off("data", onData)
      child.off("exit", onExit)
      resume(result)
    }
    const onData = (chunk: Buffer) => {
      if (chunk.toString().includes("Server is ready")) done(Effect.void)
    }
    const onExit = (code: number | null) =>
      done(
        Effect.fail(
          new TestServerError({ reason: `nats-server exited before ready (code ${code})` })
        )
      )
    child.stderr?.on("data", onData)
    child.on("exit", onExit)
  }).pipe(
    Effect.timeoutOrElse({
      duration: "10 seconds",
      orElse: () => Effect.fail(new TestServerError({ reason: "nats-server not ready in 10s" }))
    })
  )

const stopChild = (child: ChildProcess): Effect.Effect<void> =>
  Effect.callback<void>((resume) => {
    if (child.exitCode !== null || child.signalCode !== null) return resume(Effect.void)
    child.once("exit", () => resume(Effect.void))
    child.kill()
  })

export const startWith = (
  options: TestServerOptions = {}
): Effect.Effect<TestServer, TestServerError, Scope.Scope> =>
  preflightOnce.pipe(
    Effect.andThen(freePort),
    Effect.flatMap((port) =>
      Effect.acquireRelease(
        Effect.gen(function*() {
          const storeDir = mkdtempSync(join(tmpdir(), "effect-nats-js-"))
          const args = [
            "-a",
            "127.0.0.1",
            "-p",
            String(port)
          ]
          if (options.jetStream !== false) args.push("-js", "-sd", storeDir)
          if (options.name !== undefined) args.push("--server_name", options.name)
          if (options.tls !== undefined) {
            args.push("--tls", "--tlscert", options.tls.certFile, "--tlskey", options.tls.keyFile)
          }
          const child = spawn("nats-server", args, {
            stdio: ["ignore", "ignore", "pipe"],
            windowsHide: true
          })
          yield* waitReady(child)
          return {
            child,
            storeDir,
            server: { port, url: `127.0.0.1:${port}`, stop: stopChild(child) }
          }
        }),
        ({ child, storeDir }) =>
          Effect.callback<void>((resume) => {
            const finish = () => {
              try {
                rmSync(storeDir, { recursive: true, force: true })
              } catch {
                // best-effort temp cleanup
              }
              resume(Effect.void)
            }
            if (child.exitCode !== null || child.signalCode !== null) return finish()
            child.once("exit", finish)
            child.kill()
          })
      )
    ),
    Effect.map(({ server }) => server)
  )

export const start: Effect.Effect<TestServer, TestServerError, Scope.Scope> = startWith()
