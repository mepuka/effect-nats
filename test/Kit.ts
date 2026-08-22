/**
 * Test interpreters at the Connection seam: an in-memory fake of the
 * underlying client boundary with explicit behavior controls and observation
 * surfaces. No servers, no wall time.
 */
import { Cause, Clock, Effect, Layer, Queue } from "effect"
import type { Interface as ConnectorInterface } from "../src/internal/Connector.ts"
import { Connector } from "../src/internal/Connector.ts"
import type { ConnectIssue } from "../src/internal/Errors.ts"
import type { Connection, ServerStatus } from "../src/internal/NatsTransport.ts"

export interface FakeConnectionObserved {
  flushCalls: number
  drainCalls: number
  closeCalls: number
  statusIterations: number
  /** Generator terminations (return/interrupt), proving exactly-once release. */
  statusReturnCalls: number
}

export type StubMode = "ok" | "hang" | "fail"

export interface FakeConnection {
  readonly connection: Connection
  /** Interaction log and leak detectors asserted by tests. */
  readonly observed: FakeConnectionObserved
  /** Behavior controls for the next invocation of each boundary call. */
  readonly controls: {
    flush: StubMode
    drain: StubMode
    close: StubMode
  }
  /** Pushes one status frame to the status feed. */
  readonly deliverStatus: (status: ServerStatus) => Effect.Effect<void>
  /** Ends the status feed, as the real client does on close. */
  readonly endStatus: Effect.Effect<void>
  /** Marks the connection closed, as the real client does after close. */
  readonly markClosed: Effect.Effect<void>
  /** Signals once the status iterator has been entered (first pull pending). */
  readonly statusStarted: Queue.Queue<void>
}

export const makeFakeConnection = (): Effect.Effect<FakeConnection> =>
  Effect.gen(function*() {
    const statusQueue = yield* Queue.bounded<ServerStatus, Cause.Done>(64)
    const statusStarted = yield* Queue.bounded<void>(1)

    const observed: FakeConnectionObserved = {
      flushCalls: 0,
      drainCalls: 0,
      closeCalls: 0,
      statusIterations: 0,
      statusReturnCalls: 0
    }
    const controls = { flush: "ok" as StubMode, drain: "ok" as StubMode, close: "ok" as StubMode }
    let closed = false

    const stubbed = (mode: StubMode): Promise<void> =>
      mode === "fail"
        ? Promise.reject(new Error("stub failure"))
        : mode === "hang"
        ? new Promise(() => {})
        : Promise.resolve()

    const connection: Connection = {
      closed: () => Promise.resolve(undefined),
      close: () => {
        observed.closeCalls++
        return stubbed(controls.close).then(() => {
          closed = true
        })
      },
      drain: () => {
        observed.drainCalls++
        return stubbed(controls.drain).then(() => {
          closed = true
        })
      },
      flush: () => {
        observed.flushCalls++
        return stubbed(controls.flush)
      },
      isClosed: () => closed,
      isDraining: () => false,
      // Manual iterator mirroring the upstream queued-iterator contract:
      // a parked next() stays pending until a delivery or feed end; end
      // drains what was already admitted, then completes; return() settles
      // immediately and is observable exactly once.
      status: () => ({
        [Symbol.asyncIterator]() {
          observed.statusIterations++
          Queue.offerUnsafe(statusStarted, undefined)
          let returned = false
          const done = { value: undefined, done: true } as const
          return {
            next: (): Promise<IteratorResult<ServerStatus>> =>
              returned
                ? Promise.resolve(done)
                : Effect.runPromise(Queue.take(statusQueue)).then(
                  (value) => ({ value, done: false }) as IteratorResult<ServerStatus>,
                  () => done
                ),
            return: (): Promise<IteratorResult<ServerStatus>> => {
              observed.statusReturnCalls++
              returned = true
              return Promise.resolve(done)
            }
          }
        }
      })
    }

    return {
      connection,
      observed,
      controls,
      deliverStatus: (status) => Queue.offerUnsafe(statusQueue, status) ? Effect.void : Effect.void,
      endStatus: Queue.end(statusQueue),
      markClosed: Effect.sync(() => {
        closed = true
      }),
      statusStarted
    }
  })

export const statusOf = (type: string): ServerStatus => ({ type })

/**
 * Test interpreter at the Connector seam: a scripted fake of the
 * connect boundary. Outcomes are consumed in order and the last one repeats,
 * so unbounded retry loops meet a deterministic wall.
 */
export type ConnectOutcome = ConnectIssue | "succeed"

export interface FakeConnectObserved {
  connectCalls: number
  /** Virtual-time milliseconds captured inside each attempt, in order. */
  readonly attemptTimes: Array<number>
}

export interface FakeConnect {
  readonly connector: ConnectorInterface
  /** Ready-made layer satisfying the Connector service tag. */
  readonly layer: Layer.Layer<Connector>
  readonly observed: FakeConnectObserved
  /** Full harnesses behind each "succeed" outcome, in acquisition order. */
  readonly connections: Array<FakeConnection>
  /** Signals after the first recorded attempt; a deterministic attempt latch. */
  readonly firstAttempt: Queue.Queue<void>
}

export const makeFakeConnect = (
  script: ReadonlyArray<ConnectOutcome>
): Effect.Effect<FakeConnect> =>
  Effect.gen(function*() {
    if (script.length === 0) {
      return yield* Effect.die("makeFakeConnect requires a non-empty script")
    }

    const observed: FakeConnectObserved = { connectCalls: 0, attemptTimes: [] }
    const connections: Array<FakeConnection> = []
    const firstAttempt = yield* Queue.bounded<void>(1)

    let pending = [...script]
    const connector: ConnectorInterface = {
      connect: () =>
        Effect.gen(function*() {
          observed.connectCalls++
          observed.attemptTimes.push(yield* Clock.currentTimeMillis)
          yield* Effect.sync(() => {
            Queue.offerUnsafe(firstAttempt, undefined)
          })
          const outcome = pending.length > 1 ? pending.shift() : pending[0]
          if (outcome === undefined) {
            return yield* Effect.die("fake connect script drained unexpectedly")
          }
          if (outcome === "succeed") {
            const fake = yield* makeFakeConnection()
            connections.push(fake)
            return fake.connection
          }
          return yield* outcome
        })
    }

    return {
      connector,
      layer: Layer.succeed(Connector, connector),
      observed,
      connections,
      firstAttempt
    }
  })
