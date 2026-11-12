// -*- mode: Bluespec; -*-
// Farm scheduler protocol (sim/README.md) for one derivation. Messages are
// sets in flight (reordered, duplicated); crashes drop them. Scheduler state
// is soft and rebuilt from worker session reports.

module scheduler {

  pure val CLIENTS: Set[str] = Set("c1", "c2")
  pure val WORKERS: Set[str] = Set("w1", "w2")
  pure val SLOTS: int = 1
  pure val MAX_CRASHES: int = 2

  const PUSH: bool                // Expect{drv} reserves the slot; Build without it bounces
  const FENCED_PUBLISH: bool      // cache rejects a second publish
  const REPORT_ON_RECONNECT: bool // session (re)open carries running/expecting
  // What happens to a Want no connected worker can take. true: the scheduler
  // drops it and the client fails (old behaviour). false: it stays queued
  // until a worker session is back and Unplaceable is only a warning.
  const UNPLACEABLE_TERMINAL: bool
  // A reconnecting worker reports a drv the restarted scheduler meanwhile
  // placed elsewhere: true = keep the reporter and Revoke the other (kills
  // its build), false = leave both running (old behaviour).
  const REVOKE_DUP: bool
  // A reconnecting worker reports a slot it only reserved, and the scheduler
  // treats it like a running build (old behaviour). false: it reports only
  // builds a client has claimed and drops its reservations.
  const HELLO_STEAL: bool
  // A Done names its assignment and retires only that one. false: it
  // retires whatever the worker holds for the drv (old behaviour).
  const LEASES: bool

  type CPhase = CIdle | CQueued | CAssigned(str) | CSent(str) | CDone | CFailed
  type Entry = { worker: str, followers: Set[str], leased: bool, lease: int }
  type WState = { up: bool, running: bool, expecting: bool, attached: Set[str], built: bool, session: bool, lease: int }

  var cl: str -> CPhase
  var entry: List[Entry]              // Option
  var free: str -> int
  var ws: str -> WState
  var published: int
  var publishedBy: Set[str]
  var mSchedule: Set[str]             // client -> scheduler
  var mAssigned: Set[(str, str)]      // scheduler -> client (client, worker)
  var mExpect: Set[(str, int)]        // scheduler -> worker (worker, assignment)
  var mBuild: Set[(str, str)]         // client -> worker, carries lease
  var mResult: Set[str]               // worker -> client
  var mDone: Set[(str, int)]          // worker -> scheduler (worker, assignment)
  var mRetry: Set[str]                // -> client: UNAVAILABLE
  var mRevoke: Set[(str, int)]        // scheduler -> worker: stop building
  var seq: int                        // last assignment number
  var crashes: int
  var stolen: bool                    // a claimed build was revoked for a reservation

  pure def w0: WState = { up: true, running: false, expecting: false, attached: Set(), built: false, session: true, lease: -1 }

  action init = all {
    cl' = CLIENTS.mapBy(_ => CIdle),
    entry' = [],
    free' = WORKERS.mapBy(_ => SLOTS),
    ws' = WORKERS.mapBy(_ => w0),
    published' = 0,
    publishedBy' = Set(),
    mSchedule' = Set(), mAssigned' = Set(), mExpect' = Set(), mBuild' = Set(),
    mResult' = Set(), mDone' = Set(), mRetry' = Set(), mRevoke' = Set(),
    crashes' = 0, stolen' = false, seq' = 0,
  }

  action keepMsgs = all {
    mSchedule' = mSchedule, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild,
    mResult' = mResult, mDone' = mDone, mRetry' = mRetry, mRevoke' = mRevoke,
  }
  action keepCache = all { published' = published, publishedBy' = publishedBy }
  action keepSched = all { entry' = entry, free' = free }

  def busy(w: str): bool = ws.get(w).running or ws.get(w).expecting

  // ---- client ----

  action schedule(c: str): bool = all {
    cl.get(c) == CIdle,
    cl' = cl.set(c, CQueued),
    mSchedule' = mSchedule.union(Set(c)),
    mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mDone' = mDone, mRetry' = mRetry, mRevoke' = mRevoke,
    keepSched, keepCache, ws' = ws, crashes' = crashes, stolen' = stolen, seq' = seq,
  }

  action recvAssigned(c: str, w: str): bool = all {
    mAssigned.contains((c, w)),
    cl.get(c) == CQueued,
    cl' = cl.set(c, CSent(w)),
    mAssigned' = mAssigned.exclude(Set((c, w))),
    mBuild' = mBuild.union(Set((c, w))),
    mSchedule' = mSchedule, mExpect' = mExpect, mResult' = mResult, mDone' = mDone, mRetry' = mRetry, mRevoke' = mRevoke,
    keepSched, keepCache, ws' = ws, crashes' = crashes, stolen' = stolen, seq' = seq,
  }

  action recvResult(c: str): bool = all {
    mResult.contains(c),
    cl' = cl.set(c, CDone),
    mResult' = mResult.exclude(Set(c)),
    mSchedule' = mSchedule, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mDone' = mDone, mRetry' = mRetry, mRevoke' = mRevoke,
    keepSched, keepCache, ws' = ws, crashes' = crashes, stolen' = stolen, seq' = seq,
  }

  action recvRetry(c: str): bool = all {
    mRetry.contains(c),
    cl.get(c) != CDone,
    cl' = cl.set(c, CQueued),
    mRetry' = mRetry.exclude(Set(c)),
    mSchedule' = mSchedule.union(Set(c)),
    mAssigned' = mAssigned.filter(p => p._1 != c),
    mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mDone' = mDone, mRevoke' = mRevoke,
    keepSched, keepCache, ws' = ws, crashes' = crashes, stolen' = stolen, seq' = seq,
  }

  // ---- scheduler ----

  def cached: bool = published > 0

  action recvSchedule(c: str): bool = all {
    mSchedule.contains(c),
    mSchedule' = mSchedule.exclude(Set(c)),
    if (cached) all {
        mResult' = mResult.union(Set(c)),
      mAssigned' = mAssigned, mExpect' = mExpect, seq' = seq,
      keepSched,
    } else if (entry.length() == 1) all {
      val e = entry[0]
      all {
        mAssigned' = mAssigned.union(Set((c, e.worker))),
        entry' = [{ ...e, followers: e.followers.union(Set(c)) }],
        free' = free,
        mResult' = mResult, mExpect' = mExpect, seq' = seq,
      }
    } else all {
      // No eligible worker: this branch is disabled and the Want waits in
      // mSchedule (advisory Unplaceable). The terminal variant is below.
      nondet w = WORKERS.filter(k => free.get(k) > 0 and ws.get(k).session).oneOf()
      all {
        entry' = [{ worker: w, followers: Set(c), leased: true, lease: seq + 1 }],
        free' = free.set(w, free.get(w) - 1),
        mAssigned' = mAssigned.union(Set((c, w))),
        mExpect' = if (PUSH) mExpect.union(Set((w, seq + 1))) else mExpect,
        mResult' = mResult, seq' = seq + 1,
      }
    },
    mBuild' = mBuild, mDone' = mDone, mRetry' = mRetry, mRevoke' = mRevoke,
    keepCache, ws' = ws, cl' = cl, crashes' = crashes, stolen' = stolen,
  }

  // Old behaviour: nobody connected can take it => tell the client no and forget it.
  action rejectUnplaceable(c: str): bool = all {
    UNPLACEABLE_TERMINAL,
    mSchedule.contains(c), not(cached), entry.length() == 0,
    WORKERS.filter(k => free.get(k) > 0 and ws.get(k).session).size() == 0,
    mSchedule' = mSchedule.exclude(Set(c)),
    cl' = cl.set(c, CFailed),
    mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mDone' = mDone, mRetry' = mRetry, mRevoke' = mRevoke,
    keepSched, keepCache, ws' = ws, crashes' = crashes, stolen' = stolen, seq' = seq,
  }

  action recvDone(w: str, l: int): bool = all {
    mDone.contains((w, l)),
    mDone' = mDone.exclude(Set((w, l))),
    val mine = entry.length() == 1 and entry[0].worker == w and (not(LEASES) or entry[0].lease == l)
    all {
      free' = if (mine) free.set(w, free.get(w) + 1) else free,
      entry' = if (mine) [] else entry,
    },
    mSchedule' = mSchedule, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mRetry' = mRetry, mRevoke' = mRevoke,
    keepCache, ws' = ws, cl' = cl, crashes' = crashes, stolen' = stolen, seq' = seq,
  }

  action sessionLost(w: str): bool = all {
    not(ws.get(w).session) or not(ws.get(w).up),
    entry.length() == 1 and entry[0].worker == w,
    entry' = [],
    free' = free.set(w, SLOTS),
    mRetry' = mRetry.union(entry[0].followers),
    mSchedule' = mSchedule, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mDone' = mDone, mRevoke' = mRevoke,
    keepCache, ws' = ws, cl' = cl, crashes' = crashes, stolen' = stolen, seq' = seq,
  }

  // Scheduler restarts empty. Worker sessions drop and come back in
  // sessionsBack, so a client's re-Want can arrive before any worker has.
  action schedCrash: bool = all {
    crashes < MAX_CRASHES,
    crashes' = crashes + 1,
    entry' = [],
    free' = WORKERS.mapBy(_ => SLOTS),
    mAssigned' = Set(), mExpect' = Set(), mDone' = Set(),
    mRetry' = mRetry.union(CLIENTS.filter(c => cl.get(c) == CQueued)),
    mSchedule' = Set(), mBuild' = mBuild, mResult' = mResult, mRevoke' = Set(),
    keepCache, ws' = WORKERS.mapBy(k => { ...ws.get(k), session: false }), cl' = cl, stolen' = stolen, seq' = seq,
  }

  // One worker re-opens its session and reports what it runs. If the
  // scheduler placed the drv elsewhere in the meantime, the reporter keeps
  // it and the other one is revoked: its clients are retried and attach to
  // the reporter.
  action sessionBack(w: str): bool = {
    val s = ws.get(w)
    val runs = REPORT_ON_RECONNECT and (s.running or (HELLO_STEAL and s.expecting))
    val other = entry.length() == 1 and entry[0].worker != w
    all {
      s.up and not(s.session),
      ws' = ws.set(w, { ...s, session: true, expecting: s.expecting and HELLO_STEAL, lease: if (busy(w) and (s.running or HELLO_STEAL)) s.lease else -1 }),
      entry' = if (runs and (entry.length() == 0 or (other and REVOKE_DUP)))
                 [{ worker: w, followers: s.attached.union(if (other) entry[0].followers else Set()), leased: true, lease: s.lease }]
               else entry,
      // The revoked worker's slot is free again at once.
      free' = if (runs and other and REVOKE_DUP) free.set(w, SLOTS - 1).set(entry[0].worker, SLOTS)
              else free.set(w, if (runs) SLOTS - 1 else SLOTS),
      mRevoke' = if (runs and other and REVOKE_DUP) mRevoke.union(Set((entry[0].worker, entry[0].lease))) else mRevoke,
      mAssigned' = if (runs and other and REVOKE_DUP) mAssigned.union(entry[0].followers.map(c => (c, w))) else mAssigned,
      stolen' = (stolen or (runs and other and REVOKE_DUP and not(s.running) and ws.get(entry[0].worker).running)),
      mSchedule' = mSchedule, mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mDone' = mDone, mRetry' = mRetry,
      keepCache, cl' = cl, crashes' = crashes, seq' = seq,
    }
  }

  // Kills the build. Attached clients get UNAVAILABLE and re-ask, and the
  // worker answers with a Done for what it dropped. The worker stream is
  // ordered: a Revoke comes after the Expects sent before it.
  action recvRevoke(w: str, l: int): bool = all {
    mRevoke.contains((w, l)), not(mExpect.exists(p => p._1 == w and p._2 <= l)),
    mRevoke' = mRevoke.exclude(Set((w, l))),
    ws' = ws.set(w, { ...ws.get(w), running: false, expecting: false, attached: Set(), lease: -1 }),
    mRetry' = mRetry.union(ws.get(w).attached),
    mDone' = if (busy(w) and ws.get(w).session) mDone.union(Set((w, ws.get(w).lease))) else mDone,
    mSchedule' = mSchedule, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult,
    keepSched, keepCache, cl' = cl, crashes' = crashes, stolen' = stolen, seq' = seq,
  }

  // ---- worker ----

  def startOn(w: str, who: Set[str]): WState = { ...ws.get(w), running: true, expecting: false, attached: ws.get(w).attached.union(who) }

  action recvExpect(w: str, l: int): bool = all {
    mExpect.contains((w, l)), not(mRevoke.exists(p => p._1 == w and p._2 < l)),
    mExpect' = mExpect.exclude(Set((w, l))),
    ws' = ws.set(w, if (ws.get(w).up and not(ws.get(w).built) and not(ws.get(w).running)) { ...ws.get(w), expecting: true, lease: l } else ws.get(w)),
    mSchedule' = mSchedule, mAssigned' = mAssigned, mBuild' = mBuild, mResult' = mResult, mDone' = mDone, mRetry' = mRetry, mRevoke' = mRevoke,
    keepSched, keepCache, cl' = cl, crashes' = crashes, stolen' = stolen, seq' = seq,
  }

  action recvBuild(c: str, w: str): bool = all {
    mBuild.contains((c, w)),
    mBuild' = mBuild.exclude(Set((c, w))),
    val s = ws.get(w)
    if (not(s.up)) all {
      ws' = ws, mResult' = mResult, mRetry' = mRetry.union(Set(c)),
    } else if (s.built or cached) all {
      ws' = ws, mResult' = mResult.union(Set(c)), mRetry' = mRetry,
    } else if (s.running or s.expecting or not(PUSH)) all {
      ws' = ws.set(w, startOn(w, Set(c))),
      mResult' = mResult, mRetry' = mRetry,
    } else all {
      ws' = ws, mResult' = mResult, mRetry' = mRetry.union(Set(c)),
    },
    mSchedule' = mSchedule, mAssigned' = mAssigned, mExpect' = mExpect, mDone' = mDone, mRevoke' = mRevoke,
    keepSched, keepCache, cl' = cl, crashes' = crashes, stolen' = stolen, seq' = seq,
  }

  // Nobody claimed the reserved slot in time: the worker gives it back.
  action expectTimeout(w: str): bool = all {
    ws.get(w).up, ws.get(w).expecting,
    ws' = ws.set(w, { ...ws.get(w), expecting: false, lease: -1 }),
    mDone' = if (ws.get(w).session) mDone.union(Set((w, ws.get(w).lease))) else mDone,
    mSchedule' = mSchedule, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mRetry' = mRetry, mRevoke' = mRevoke,
    keepSched, keepCache, cl' = cl, crashes' = crashes, stolen' = stolen, seq' = seq,
  }

  action finish(w: str): bool = all {
    ws.get(w).up and ws.get(w).running,
    val accepted = not(FENCED_PUBLISH) or published == 0
    all {
      published' = if (accepted) published + 1 else published,
      publishedBy' = if (accepted) publishedBy.union(Set(w)) else publishedBy,
      ws' = ws.set(w, { ...ws.get(w), running: false, expecting: false, built: true, attached: Set(), lease: -1 }),
      mResult' = mResult.union(ws.get(w).attached),
      mDone' = if (ws.get(w).session) mDone.union(Set((w, ws.get(w).lease))) else mDone,
    },
    mSchedule' = mSchedule, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mRetry' = mRetry, mRevoke' = mRevoke,
    keepSched, cl' = cl, crashes' = crashes, stolen' = stolen, seq' = seq,
  }

  action workerCrash(w: str): bool = all {
    crashes < MAX_CRASHES, ws.get(w).up,
    crashes' = crashes + 1,
    ws' = ws.set(w, { ...ws.get(w), up: false, running: false, expecting: false, attached: Set(), session: false, lease: -1 }),
    mRetry' = mRetry.union(ws.get(w).attached),
    mExpect' = mExpect.filter(p => p._1 != w),
    mSchedule' = mSchedule, mAssigned' = mAssigned, mBuild' = mBuild, mResult' = mResult,
    // the session stream dies with the worker
    mDone' = mDone.filter(p => p._1 != w), mRevoke' = mRevoke.filter(p => p._1 != w),
    keepSched, keepCache, cl' = cl, stolen' = stolen, seq' = seq,
  }

  action workerBack(w: str): bool = all {
    not(ws.get(w).up),
    ws' = ws.set(w, { ...ws.get(w), up: true, session: true }),
    val stale = entry.length() == 1 and entry[0].worker == w
    all {
      entry' = if (stale) [] else entry,
      free' = free.set(w, SLOTS),
      mRetry' = if (stale) mRetry.union(entry[0].followers) else mRetry,
    },
    mSchedule' = mSchedule, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mDone' = mDone, mRevoke' = mRevoke,
    keepCache, cl' = cl, crashes' = crashes, stolen' = stolen, seq' = seq,
  }

  action step = {
    nondet c = CLIENTS.oneOf()
    nondet w = WORKERS.oneOf()
    nondet l = (-1).to(seq).oneOf()
    any {
      schedule(c), recvAssigned(c, w), recvResult(c), recvRetry(c),
      recvSchedule(c), rejectUnplaceable(c), recvDone(w, l), sessionLost(w), schedCrash, sessionBack(w),
      recvRevoke(w, l), recvExpect(w, l), recvBuild(c, w), expectTimeout(w), finish(w), workerCrash(w), workerBack(w),
    }
  }

  action stepCalm = {
    nondet c = CLIENTS.oneOf()
    nondet w = WORKERS.oneOf()
    nondet l = (-1).to(seq).oneOf()
    any {
      schedule(c), recvAssigned(c, w), recvResult(c),
      recvSchedule(c), recvDone(w, l), recvExpect(w, l), recvBuild(c, w), finish(w),
    }
  }

  // ---- scripted traces ----
  // Random walks rarely get a second build claimed, so the traces that need
  // one are spelled out. Each step has one enabled choice.

  action expectAny = { nondet p = mExpect.oneOf() recvExpect(p._1, p._2) }
  action revokeAny = { nondet p = mRevoke.oneOf() recvRevoke(p._1, p._2) }
  action doneAny = { nondet p = mDone.oneOf() recvDone(p._1, p._2) }
  action crashPlaced = { nondet w = WORKERS.filter(w => entry.length() == 1 and entry[0].worker == w).oneOf() workerCrash(w) }
  action lostPlaced = { nondet w = WORKERS.filter(w => entry.length() == 1 and entry[0].worker == w).oneOf() sessionLost(w) }
  action assignedAny(c: str): bool = { nondet p = mAssigned.filter(p => p._1 == c).oneOf() recvAssigned(c, p._2) }
  action buildAny(c: str): bool = { nondet p = mBuild.filter(p => p._1 == c).oneOf() recvBuild(c, p._2) }
  action backIdle = { nondet w = WORKERS.filter(w => not(ws.get(w).session) and not(busy(w))).oneOf() sessionBack(w) }
  action backBusy = { nondet w = WORKERS.filter(w => not(ws.get(w).session) and busy(w)).oneOf() sessionBack(w) }
  action finishAny = { nondet w = WORKERS.filter(w => ws.get(w).running).oneOf() finish(w) }

  // The scheduler restarts while a build runs and places the drv again
  // before the first worker has reported back.
  run dupBuildTest =
    init
      .then(schedule("c1")).then(recvSchedule("c1")).then(expectAny)
      .then(assignedAny("c1")).then(buildAny("c1"))
      .then(schedCrash).then(backIdle)
      .then(schedule("c2")).then(recvSchedule("c2")).then(expectAny)
      .then(assignedAny("c2")).then(buildAny("c2"))
      .then(backBusy)
      .expect(oneBuilder == (REPORT_ON_RECONNECT and REVOKE_DUP))
      .then(finishAny).then(finishAny)
      .expect(oneResult == FENCED_PUBLISH)

  // The scheduler restarts after it reserved a slot nobody claimed yet. The
  // client asks again and builds elsewhere, then the first worker reports.
  run helloStealTest =
    init
      .then(schedule("c1")).then(recvSchedule("c1")).then(expectAny)
      .then(schedCrash).then(backIdle)
      .then(recvRetry("c1")).then(recvSchedule("c1")).then(expectAny)
      .then(assignedAny("c1")).then(buildAny("c1"))
      .then(backBusy)
      .expect(stolen == (REPORT_ON_RECONNECT and HELLO_STEAL))

  // The scheduler revokes a build and places the drv on the same worker
  // again before the worker has read the Revoke. The worker's Done for the
  // revoked build then arrives after the new assignment.
  run staleDoneTest =
    init
      .then(schedule("c1")).then(recvSchedule("c1")).then(expectAny)
      .then(assignedAny("c1")).then(buildAny("c1"))
      .then(schedCrash).then(backIdle)
      .then(schedule("c2")).then(recvSchedule("c2")).then(expectAny)
      .then(assignedAny("c2")).then(buildAny("c2"))
      .then(backBusy)
      .then(crashPlaced).then(lostPlaced)
      .then(recvRetry("c1")).then(recvSchedule("c1"))
      .then(revokeAny).then(expectAny).then(doneAny)
      .expect(noOversubscribe == LEASES)

  // ---- properties ----

  val doneImpliesPublished = CLIENTS.forall(c => cl.get(c) == CDone implies published > 0)

  val slotsSane = WORKERS.forall(w => free.get(w) >= 0 and free.get(w) <= SLOTS)

  val noOversubscribe = REPORT_ON_RECONNECT implies
    WORKERS.forall(w => (ws.get(w).session and busy(w) and not(mDone.exists(p => p._1 == w)) and not(mRevoke.exists(p => p._1 == w)))
      implies free.get(w) <= SLOTS - 1)

  val singlePublish = published <= 1

  // the entry's worker still owes a Done, a bounce or a session drop
  val entryLive = entry.length() == 1 implies (
    val w = entry[0].worker
    ws.get(w).running or ws.get(w).expecting or ws.get(w).built or mDone.exists(p => p._1 == w) or mExpect.exists(p => p._1 == w) or mBuild.exists(p => p._2 == w)
      or mAssigned.exists(p => p._2 == w) or not(ws.get(w).session) or not(ws.get(w).up)
      or entry[0].followers.exists(c => cl.get(c) == CQueued or mRetry.contains(c)))

  // something in flight still concerns every waiting client
  val noStrandedClient = CLIENTS.forall(c =>
    cl.get(c) == CIdle or cl.get(c) == CDone or cl.get(c) == CFailed
    or mSchedule.contains(c) or mResult.contains(c) or mRetry.contains(c)
    or mAssigned.exists(p => p._1 == c) or mBuild.exists(p => p._1 == c)
    or WORKERS.exists(w => ws.get(w).attached.contains(c))
    or (entry.length() == 1 and entry[0].followers.contains(c)))

  // A restarted scheduler may place a re-Want before the worker already
  // building it has reconnected. That second build lives only until the
  // first worker's Hello, then a Revoke is on its way. So: among workers the
  // scheduler has heard from, at most one builds and is not being revoked.
  val oneBuilder = WORKERS.filter(w => ws.get(w).running and ws.get(w).session and not(mRevoke.exists(p => p._1 == w))).size() <= 1
  // What must never happen: two finished output sets of one drv.
  val oneResult = publishedBy.size() <= 1

  // Every worker here can build the drv and crashed ones come back, so a
  // client told "unplaceable" for good was told wrong: it only saw the
  // scheduler before its workers had reconnected.
  val noSpuriousFail = CLIENTS.forall(c => cl.get(c) != CFailed)

  val noSteal = not(stolen)

  val safety = all { noSteal, doneImpliesPublished, slotsSane, singlePublish, noStrandedClient, noOversubscribe, entryLive, noSpuriousFail, oneBuilder, oneResult }
}

module schedFixed    { import scheduler(PUSH = true,  FENCED_PUBLISH = true,  REPORT_ON_RECONNECT = true,  UNPLACEABLE_TERMINAL = false, REVOKE_DUP = true, HELLO_STEAL = false, LEASES = true).* }
// each drops one mechanism; `safety` must find the hole
module schedNoPush   { import scheduler(PUSH = false, FENCED_PUBLISH = true,  REPORT_ON_RECONNECT = true,  UNPLACEABLE_TERMINAL = false, REVOKE_DUP = true, HELLO_STEAL = false, LEASES = true).* }
module schedNoReport { import scheduler(PUSH = true,  FENCED_PUBLISH = false, REPORT_ON_RECONNECT = false, UNPLACEABLE_TERMINAL = false, REVOKE_DUP = true, HELLO_STEAL = false, LEASES = true).* }
// without fencing the duplicate build after a scheduler restart publishes twice
module schedNoFence  { import scheduler(PUSH = true,  FENCED_PUBLISH = false, REPORT_ON_RECONNECT = true,  UNPLACEABLE_TERMINAL = false, REVOKE_DUP = true, HELLO_STEAL = false, LEASES = true).* }
// scheduler answers "no worker" right after a restart and drops the Want
module schedTerminal { import scheduler(PUSH = true,  FENCED_PUBLISH = true,  REPORT_ON_RECONNECT = true,  UNPLACEABLE_TERMINAL = true, REVOKE_DUP = true, HELLO_STEAL = false, LEASES = true).* }
// reconnect reports a duplicate and the scheduler lets both run
module schedNoRevoke { import scheduler(PUSH = true,  FENCED_PUBLISH = true,  REPORT_ON_RECONNECT = true,  UNPLACEABLE_TERMINAL = false, REVOKE_DUP = false, HELLO_STEAL = false, LEASES = true).* }
// a reservation nobody claimed takes the drv from a worker that builds it
module schedHelloSteal { import scheduler(PUSH = true, FENCED_PUBLISH = true, REPORT_ON_RECONNECT = true, UNPLACEABLE_TERMINAL = false, REVOKE_DUP = true, HELLO_STEAL = true, LEASES = true).* }
// a Done for a revoked build retires the assignment that replaced it
module schedNoLease { import scheduler(PUSH = true, FENCED_PUBLISH = true, REPORT_ON_RECONNECT = true, UNPLACEABLE_TERMINAL = false, REVOKE_DUP = true, HELLO_STEAL = false, LEASES = false).* }
