// -*- mode: Bluespec; -*-
// Build-claim protocol between farm workers (src/farm.hh, farmBuild) and
// niks3 (server/claims.go, TryClaim SQL) for one output set. Time is
// abstracted into guarded nondeterministic events.

module claims {

  // farm.hh Claim::Status + farmBuild control flow
  type Phase = Idle | Pending | Waiting | Building | Publishing | Done

  // what farmBuild returned to the gRPC client
  type Result = RNone | RBuilt | RAlreadyValid | RFailedElsewhere | RFailed | RUnavailable

  // niks3 ClaimHandler goroutine per stream
  type Srv = SNone | SNeed | SWaiter | SHolder(int)

  // slot: one of the host's max-jobs. outputs: built paths still on local disk.
  type Worker = { phase: Phase, token: int, conn: bool, srv: Srv, res: Result, slot: bool, outputs: bool }

  pure val WORKERS: Set[str] = Set("w1", "w2", "w3")
  // all workers share one host with this many slots, the tightest case
  pure val SLOTS: int = 1

  var ws: str -> Worker
  var present: bool      // outputs committed
  var touched: bool      // TouchClosures ran since the last cache GC sweep
  var rowToken: int      // claims row, 0 = none
  var rowStale: bool     // heartbeat_at older than 3*hb
  var nextToken: int

  pure def idle: Worker = { phase: Idle, token: 0, conn: false, srv: SNone, res: RNone, slot: false, outputs: false }

  action init = all {
    ws' = WORKERS.mapBy(_ => idle),
    present' = false,
    touched' = false,
    rowToken' = 0,
    rowStale' = false,
    nextToken' = 1,
  }

  action unchangedDb = all {
    present' = present, touched' = touched, rowToken' = rowToken, rowStale' = rowStale, nextToken' = nextToken,
  }

  def slotsUsed(m: str -> Worker): int = WORKERS.filter(k => m.get(k).slot).size()

  // pg_notify: waiters re-run tryClaim, or end with "failed" on a kind
  def woken(m: str -> Worker, kind: bool): str -> Worker =
    m.keys().mapBy(k => {
      val w = m.get(k)
      if (w.srv == SWaiter)
        if (kind) { ...w, srv: SNone, conn: false,
                    phase: if (w.phase == Waiting or w.phase == Pending) Done else w.phase,
                    res:   if (w.phase == Waiting or w.phase == Pending) RFailedElsewhere else w.res }
        else { ...w, srv: SNeed }
      else w
    })

  action startClaim(k: str): bool = {
    val w = ws.get(k)
    all {
      w.phase == Idle,
      slotsUsed(ws) < SLOTS,
      ws' = ws.set(k, { ...w, phase: Pending, conn: true, srv: SNeed, slot: true }),
      unchangedDb,
    }
  }

  // tryClaim + send one line, onLine() on the worker
  action answer(k: str): bool = {
    val w = ws.get(k)
    val fresh = w.phase == Pending or w.phase == Waiting
    all {
      w.conn, w.srv == SNeed,
      if (present) all {
        ws' = ws.set(k, if (fresh) { ...idle, phase: Done, res: RAlreadyValid } else { ...w, srv: SNone, conn: false }),
        touched' = true, present' = present, rowToken' = rowToken, rowStale' = rowStale, nextToken' = nextToken,
      } else if (rowToken == 0 or rowStale or rowToken == w.token) {
        val tok = if (rowToken != 0 and rowToken == w.token) rowToken else nextToken
        val bump = if (tok == nextToken) nextToken + 1 else nextToken
        if (fresh and not(w.slot) and slotsUsed(ws) >= SLOTS) all {
          // promoted waiter, SlotGuard::tryAcquire fails: UNAVAILABLE, ~Claim -> fail("")
          rowToken' = 0, rowStale' = false, present' = present, touched' = touched, nextToken' = bump,
          ws' = woken(ws, false).set(k, { ...idle, phase: Done, res: RUnavailable, token: tok }),
        } else all {
          rowToken' = tok, rowStale' = false, present' = present, touched' = touched, nextToken' = bump,
          ws' = ws.set(k, { ...w, srv: SHolder(tok), token: tok, slot: true, phase: if (fresh) Building else w.phase }),
        }
      } else all {
        // "wait": farmBuild drops its slot
        ws' = ws.set(k, { ...w, srv: SWaiter,
                         phase: if (w.phase == Pending) Waiting else w.phase,
                         slot: if (w.phase == Pending) false else w.slot }),
        unchangedDb,
      }
    }
  }

  // stream breaks: holdClaim returns, the row stays
  action disconnect(k: str): bool = {
    val w = ws.get(k)
    all {
      w.conn,
      ws' = ws.set(k, { ...w, conn: false, srv: SNone }),
      unchangedDb,
    }
  }

  // release by token: fail("") from ~Claim, fail(kind), or commit
  action release(k: str, w: Worker, kind: bool, setPresent: bool): bool = {
    if (rowToken == w.token and rowToken != 0) all {
      rowToken' = 0, rowStale' = false, nextToken' = nextToken,
      present' = (present or setPresent), touched' = (touched or setPresent),
      ws' = woken(ws, kind).set(k, w),
    } else all {
      unchangedDb,
      ws' = ws.set(k, w),
    }
  }

  // Claim::run() re-opens with our token
  action reconnect(k: str): bool = {
    val w = ws.get(k)
    all {
      not(w.conn),
      w.phase == Pending or w.phase == Waiting or w.phase == Building,
      ws' = ws.set(k, { ...w, conn: true, srv: SNeed }),
      unchangedDb,
    }
  }

  // giveUpAfter(): lost() -> UNAVAILABLE, or waitFor throws -> INTERNAL.
  // ~Claim's fail("") cannot reach niks3 here.
  action giveUp(k: str): bool = {
    val w = ws.get(k)
    all {
      not(w.conn),
      w.phase == Pending or w.phase == Waiting or w.phase == Building,
      ws' = ws.set(k, { ...idle, phase: Done, res: RUnavailable, token: w.token }),
      unchangedDb,
    }
  }

  // NOT_FOUND / FAILED_PRECONDITION before runBuild: ~Claim -> fail("")
  action earlyReturn(k: str): bool = {
    val w = ws.get(k)
    all {
      w.phase == Building, w.conn,
      release(k, { ...idle, phase: Done, res: RUnavailable, token: w.token }, false, false),
    }
  }

  action finishBuild(k: str, ok: bool): bool = {
    val w = ws.get(k)
    all {
      w.phase == Building,
      w.conn,   // else giveUp
      if (ok) all {
        ws' = ws.set(k, { ...w, phase: Publishing, outputs: true }),
        unchangedDb,
      } else {
        // fail(kind), a 409 still returns the BuildResult
        release(k, { ...idle, phase: Done, res: RFailed, token: w.token }, true, false)
      },
    }
  }

  // host GC under disk pressure. farmBuild temp-roots the outputs on a
  // connection held across publish, so only unrooted leftovers go.
  action gcWorker(k: str): bool = {
    val w = ws.get(k)
    all {
      w.outputs, w.phase != Publishing,
      ws' = ws.set(k, { ...w, outputs: false }),
      unchangedDb,
    }
  }

  // hook commit with token. Outputs gone -> upload fails. StaleClaim -> UNAVAILABLE.
  action publish(k: str): bool = {
    val w = ws.get(k)
    all {
      w.phase == Publishing,
      if (w.outputs and rowToken == w.token and rowToken != 0)
        release(k, { ...idle, phase: Done, res: RBuilt, token: w.token }, false, true)
      else if (not(w.outputs))
        release(k, { ...idle, phase: Done, res: RUnavailable, token: w.token }, false, false)
      else all {
        unchangedDb,
        ws' = ws.set(k, { ...idle, phase: Done, res: RUnavailable, token: w.token }),
      }
    }
  }

  // niks3 GC: only objects not touched within the retention window go
  action gcCache: bool = all {
    present, not(touched),
    present' = false, touched' = false, rowToken' = rowToken, rowStale' = rowStale, nextToken' = nextToken,
    ws' = ws,
  }

  // Retention window passes. A dependant's claim runs TouchClosures(inputs)
  // right before substituting, so results older than the window are moot.
  action retentionExpires: bool = all {
    touched,
    WORKERS.forall(k => ws.get(k).phase == Idle or ws.get(k).phase == Done),
    touched' = false, present' = present, rowToken' = rowToken, rowStale' = rowStale, nextToken' = nextToken,
    ws' = WORKERS.mapBy(_ => idle),
  }

  // streams gone, no cleanup
  action niks3Crash: bool = all {
    WORKERS.exists(k => ws.get(k).conn),
    ws' = ws.keys().mapBy(k => { ...ws.get(k), conn: false, srv: SNone }),
    unchangedDb,
  }

  // only when no live holdClaim refreshes the row
  action rowAges: bool = all {
    rowToken != 0, not(rowStale),
    not(WORKERS.exists(k => ws.get(k).srv == SHolder(rowToken))),
    rowStale' = true, rowToken' = rowToken, present' = present, touched' = touched, nextToken' = nextToken,
    ws' = ws,
  }

  // a Done worker's client comes back with a new RPC
  action again(k: str): bool = {
    val w = ws.get(k)
    all { w.phase == Done, ws' = ws.set(k, idle), unchangedDb }
  }

  action waiterTick(k: str): bool = {
    val w = ws.get(k)
    all { w.conn, w.srv == SWaiter, ws' = ws.set(k, { ...w, srv: SNeed }), unchangedDb }
  }

  action step = {
    nondet k = WORKERS.oneOf()
    nondet ok = Set(true, false).oneOf()
    any {
      startClaim(k), answer(k), disconnect(k), reconnect(k), giveUp(k), earlyReturn(k),
      finishBuild(k, ok), publish(k), waiterTick(k), gcWorker(k), again(k),
      niks3Crash, rowAges, gcCache, retentionExpires,
    }
  }

  // streams drop and come back within the staleness window, niks3 stays up
  action stepBlips = {
    nondet k = WORKERS.oneOf()
    nondet ok = Set(true, false).oneOf()
    any {
      startClaim(k), answer(k), disconnect(k), reconnect(k), earlyReturn(k),
      finishBuild(k, ok), publish(k), waiterTick(k),
    }
  }

  // a client told Built/AlreadyValid can fetch the outputs (until retention lapses)
  val builtImpliesPresent =
    WORKERS.forall(k => (ws.get(k).res == RBuilt or ws.get(k).res == RAlreadyValid) implies present)

  val slotsBounded = slotsUsed(ws) <= SLOTS

  // nobody keeps the row alive without a slot to build in
  val holderHasSlot =
    WORKERS.forall(k => (ws.get(k).phase == Building or ws.get(k).phase == Publishing) implies ws.get(k).slot)

  // built outputs survive on the worker until publish has run
  val outputsRooted =
    WORKERS.forall(k => ws.get(k).phase == Publishing implies ws.get(k).outputs)

  // at most one worker holds the live token
  val oneFencedHolder =
    WORKERS.filter(k => ws.get(k).phase == Building and ws.get(k).token == rowToken and rowToken != 0).size() <= 1

  // dedup, checked under stepBlips (staleness legitimately breaks it)
  val oneBuilder =
    WORKERS.filter(k => ws.get(k).phase == Building or ws.get(k).phase == Publishing).size() <= 1

  val failedElsewhereHasCause =
    WORKERS.exists(k => ws.get(k).res == RFailedElsewhere) implies WORKERS.exists(k => ws.get(k).res == RFailed)

  // no connected waiter parked on a free key
  val noOrphanWaiter =
    (rowToken == 0 and not(present)) implies not(WORKERS.exists(k => ws.get(k).srv == SWaiter))

  val safety = all {
    builtImpliesPresent, oneFencedHolder, failedElsewhereHasCause, noOrphanWaiter,
    slotsBounded, holderHasSlot, outputsRooted,
  }
}
