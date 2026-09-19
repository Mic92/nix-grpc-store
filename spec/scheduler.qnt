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

  type CPhase = CIdle | CQueued | CAssigned(str) | CSent(str) | CDone | CFailed
  type Entry = { worker: str, followers: Set[str], leased: bool }
  type WState = { up: bool, running: bool, expecting: bool, attached: Set[str], built: bool, session: bool }

  var cl: str -> CPhase
  var entry: List[Entry]              // Option
  var free: str -> int
  var ws: str -> WState
  var published: int
  var publishedBy: Set[str]
  var mSchedule: Set[str]             // client -> scheduler
  var mAssigned: Set[(str, str)]      // scheduler -> client (client, worker)
  var mExpect: Set[str]               // scheduler -> worker
  var mBuild: Set[(str, str)]         // client -> worker, carries lease
  var mResult: Set[str]               // worker -> client
  var mDone: Set[str]                 // worker -> scheduler
  var mRetry: Set[str]                // -> client: UNAVAILABLE
  var crashes: int

  pure def w0: WState = { up: true, running: false, expecting: false, attached: Set(), built: false, session: true }

  action init = all {
    cl' = CLIENTS.mapBy(_ => CIdle),
    entry' = [],
    free' = WORKERS.mapBy(_ => SLOTS),
    ws' = WORKERS.mapBy(_ => w0),
    published' = 0,
    publishedBy' = Set(),
    mSchedule' = Set(), mAssigned' = Set(), mExpect' = Set(), mBuild' = Set(),
    mResult' = Set(), mDone' = Set(), mRetry' = Set(),
    crashes' = 0,
  }

  action keepMsgs = all {
    mSchedule' = mSchedule, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild,
    mResult' = mResult, mDone' = mDone, mRetry' = mRetry,
  }
  action keepCache = all { published' = published, publishedBy' = publishedBy }
  action keepSched = all { entry' = entry, free' = free }

  // ---- client ----

  action schedule(c: str): bool = all {
    cl.get(c) == CIdle,
    cl' = cl.set(c, CQueued),
    mSchedule' = mSchedule.union(Set(c)),
    mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mDone' = mDone, mRetry' = mRetry,
    keepSched, keepCache, ws' = ws, crashes' = crashes,
  }

  action recvAssigned(c: str, w: str): bool = all {
    mAssigned.contains((c, w)),
    cl.get(c) == CQueued,
    cl' = cl.set(c, CSent(w)),
    mAssigned' = mAssigned.exclude(Set((c, w))),
    mBuild' = mBuild.union(Set((c, w))),
    mSchedule' = mSchedule, mExpect' = mExpect, mResult' = mResult, mDone' = mDone, mRetry' = mRetry,
    keepSched, keepCache, ws' = ws, crashes' = crashes,
  }

  action recvResult(c: str): bool = all {
    mResult.contains(c),
    cl' = cl.set(c, CDone),
    mResult' = mResult.exclude(Set(c)),
    mSchedule' = mSchedule, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mDone' = mDone, mRetry' = mRetry,
    keepSched, keepCache, ws' = ws, crashes' = crashes,
  }

  action recvRetry(c: str): bool = all {
    mRetry.contains(c),
    cl.get(c) != CDone,
    cl' = cl.set(c, CQueued),
    mRetry' = mRetry.exclude(Set(c)),
    mSchedule' = mSchedule.union(Set(c)),
    mAssigned' = mAssigned.filter(p => p._1 != c),
    mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mDone' = mDone,
    keepSched, keepCache, ws' = ws, crashes' = crashes,
  }

  // ---- scheduler ----

  def cached: bool = published > 0

  action recvSchedule(c: str): bool = all {
    mSchedule.contains(c),
    mSchedule' = mSchedule.exclude(Set(c)),
    if (cached) all {
        mResult' = mResult.union(Set(c)),
      mAssigned' = mAssigned, mExpect' = mExpect,
      keepSched,
    } else if (entry.length() == 1) all {
      val e = entry[0]
      all {
        mAssigned' = mAssigned.union(Set((c, e.worker))),
        entry' = [{ ...e, followers: e.followers.union(Set(c)) }],
        free' = free,
        mResult' = mResult, mExpect' = mExpect,
      }
    } else all {
      // No eligible worker: this branch is disabled and the Want waits in
      // mSchedule (advisory Unplaceable). The terminal variant is below.
      nondet w = WORKERS.filter(k => free.get(k) > 0 and ws.get(k).session).oneOf()
      all {
        entry' = [{ worker: w, followers: Set(c), leased: true }],
        free' = free.set(w, free.get(w) - 1),
        mAssigned' = mAssigned.union(Set((c, w))),
        mExpect' = if (PUSH) mExpect.union(Set(w)) else mExpect,
        mResult' = mResult,
      }
    },
    mBuild' = mBuild, mDone' = mDone, mRetry' = mRetry,
    keepCache, ws' = ws, cl' = cl, crashes' = crashes,
  }

  // Old behaviour: nobody connected can take it => tell the client no and forget it.
  action rejectUnplaceable(c: str): bool = all {
    UNPLACEABLE_TERMINAL,
    mSchedule.contains(c), not(cached), entry.length() == 0,
    WORKERS.filter(k => free.get(k) > 0 and ws.get(k).session).size() == 0,
    mSchedule' = mSchedule.exclude(Set(c)),
    cl' = cl.set(c, CFailed),
    mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mDone' = mDone, mRetry' = mRetry,
    keepSched, keepCache, ws' = ws, crashes' = crashes,
  }

  action recvDone(w: str): bool = all {
    mDone.contains(w),
    mDone' = mDone.exclude(Set(w)),
    free' = if (entry.length() == 1 and entry[0].worker == w) free.set(w, free.get(w) + 1) else free,
    entry' = if (entry.length() == 1 and entry[0].worker == w) [] else entry,
    mSchedule' = mSchedule, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mRetry' = mRetry,
    keepCache, ws' = ws, cl' = cl, crashes' = crashes,
  }

  action sessionLost(w: str): bool = all {
    not(ws.get(w).session) or not(ws.get(w).up),
    entry.length() == 1 and entry[0].worker == w,
    entry' = [],
    free' = free.set(w, SLOTS),
    mRetry' = mRetry.union(entry[0].followers),
    mSchedule' = mSchedule, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mDone' = mDone,
    keepCache, ws' = ws, cl' = cl, crashes' = crashes,
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
    mSchedule' = Set(), mBuild' = mBuild, mResult' = mResult,
    keepCache, ws' = WORKERS.mapBy(k => { ...ws.get(k), session: false }), cl' = cl,
  }

  // Up workers re-open their sessions and report what they run.
  action sessionsBack: bool = all {
    WORKERS.exists(k => ws.get(k).up and not(ws.get(k).session)),
    ws' = WORKERS.mapBy(k => { ...ws.get(k), session: ws.get(k).up }),
    val running = WORKERS.filter(k => ws.get(k).up and not(ws.get(k).session) and (ws.get(k).running or ws.get(k).expecting))
    all {
      entry' = if (REPORT_ON_RECONNECT and entry.length() == 0 and running.size() > 0) {
                 val w = running.fold("", (a, k) => if (a == "") k else a)
                 [{ worker: w, followers: ws.get(w).attached, leased: true }]
               } else entry,
      free' = WORKERS.mapBy(k => if (REPORT_ON_RECONNECT and running.contains(k)) SLOTS - 1 else free.get(k)),
    },
    keepMsgs, keepCache, cl' = cl, crashes' = crashes,
  }

  // ---- worker ----

  def startOn(w: str, who: Set[str]): WState = { ...ws.get(w), running: true, attached: ws.get(w).attached.union(who) }

  action recvExpect(w: str): bool = all {
    mExpect.contains(w),
    mExpect' = mExpect.exclude(Set(w)),
    ws' = ws.set(w, if (ws.get(w).up and not(ws.get(w).built)) { ...startOn(w, Set()), expecting: true } else ws.get(w)),
    mSchedule' = mSchedule, mAssigned' = mAssigned, mBuild' = mBuild, mResult' = mResult, mDone' = mDone, mRetry' = mRetry,
    keepSched, keepCache, cl' = cl, crashes' = crashes,
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
      ws' = ws.set(w, { ...startOn(w, Set(c)), expecting: true }),
      mResult' = mResult, mRetry' = mRetry,
    } else all {
      ws' = ws, mResult' = mResult, mRetry' = mRetry.union(Set(c)),
    },
    mSchedule' = mSchedule, mAssigned' = mAssigned, mExpect' = mExpect, mDone' = mDone,
    keepSched, keepCache, cl' = cl, crashes' = crashes,
  }

  action finish(w: str): bool = all {
    ws.get(w).up and ws.get(w).running,
    val accepted = not(FENCED_PUBLISH) or published == 0
    all {
      published' = if (accepted) published + 1 else published,
      publishedBy' = if (accepted) publishedBy.union(Set(w)) else publishedBy,
      ws' = ws.set(w, { ...ws.get(w), running: false, expecting: false, built: true, attached: Set() }),
      mResult' = mResult.union(ws.get(w).attached),
      mDone' = if (ws.get(w).session) mDone.union(Set(w)) else mDone,
    },
    mSchedule' = mSchedule, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mRetry' = mRetry,
    keepSched, cl' = cl, crashes' = crashes,
  }

  action workerCrash(w: str): bool = all {
    crashes < MAX_CRASHES, ws.get(w).up,
    crashes' = crashes + 1,
    ws' = ws.set(w, { ...ws.get(w), up: false, running: false, expecting: false, attached: Set(), session: false }),
    mRetry' = mRetry.union(ws.get(w).attached),
    mExpect' = mExpect.exclude(Set(w)),
    mSchedule' = mSchedule, mAssigned' = mAssigned, mBuild' = mBuild, mResult' = mResult, mDone' = mDone,
    keepSched, keepCache, cl' = cl,
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
    mSchedule' = mSchedule, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mDone' = mDone,
    keepCache, cl' = cl, crashes' = crashes,
  }

  action step = {
    nondet c = CLIENTS.oneOf()
    nondet w = WORKERS.oneOf()
    any {
      schedule(c), recvAssigned(c, w), recvResult(c), recvRetry(c),
      recvSchedule(c), rejectUnplaceable(c), recvDone(w), sessionLost(w), schedCrash, sessionsBack,
      recvExpect(w), recvBuild(c, w), finish(w), workerCrash(w), workerBack(w),
    }
  }

  // Everything but scheduler restarts: worker crashes, bounces, reordering.
  action stepNoSchedCrash = {
    nondet c = CLIENTS.oneOf()
    nondet w = WORKERS.oneOf()
    any {
      schedule(c), recvAssigned(c, w), recvResult(c), recvRetry(c),
      recvSchedule(c), rejectUnplaceable(c), recvDone(w), sessionLost(w),
      recvExpect(w), recvBuild(c, w), finish(w), workerCrash(w), workerBack(w),
    }
  }

  action stepCalm = {
    nondet c = CLIENTS.oneOf()
    nondet w = WORKERS.oneOf()
    any {
      schedule(c), recvAssigned(c, w), recvResult(c),
      recvSchedule(c), recvDone(w), recvExpect(w), recvBuild(c, w), finish(w),
    }
  }

  // ---- properties ----

  val doneImpliesPublished = CLIENTS.forall(c => cl.get(c) == CDone implies published > 0)

  val slotsSane = WORKERS.forall(w => free.get(w) >= 0 and free.get(w) <= SLOTS)

  val noOversubscribe = REPORT_ON_RECONNECT implies
    WORKERS.forall(w => (ws.get(w).session and (ws.get(w).running or ws.get(w).expecting) and not(mDone.contains(w)))
      implies free.get(w) <= SLOTS - 1)

  val singlePublish = published <= 1

  // the entry's worker still owes a Done, a bounce or a session drop
  val entryLive = entry.length() == 1 implies (
    val w = entry[0].worker
    ws.get(w).running or ws.get(w).expecting or ws.get(w).built or mDone.contains(w) or mExpect.contains(w) or mBuild.exists(p => p._2 == w)
      or mAssigned.exists(p => p._2 == w) or not(ws.get(w).session) or not(ws.get(w).up)
      or entry[0].followers.exists(c => cl.get(c) == CQueued or mRetry.contains(c)))

  // something in flight still concerns every waiting client
  val noStrandedClient = CLIENTS.forall(c =>
    cl.get(c) == CIdle or cl.get(c) == CDone or cl.get(c) == CFailed
    or mSchedule.contains(c) or mResult.contains(c) or mRetry.contains(c)
    or mAssigned.exists(p => p._1 == c) or mBuild.exists(p => p._1 == c)
    or WORKERS.exists(w => ws.get(w).attached.contains(c))
    or (entry.length() == 1 and entry[0].followers.contains(c)))

  // Not in `safety`: a restarted scheduler places a re-Want on whoever has
  // reconnected while a worker that has not is still building the same drv.
  // The duplicate is wasted work, not a wrong result: singlePublish holds.
  // Checked under stepNoSchedCrash, where Expect-before-Build does keep it.
  val oneBuilder = WORKERS.filter(w => ws.get(w).running).size() <= 1

  // Every worker here can build the drv and crashed ones come back, so a
  // client told "unplaceable" for good was told wrong: it only saw the
  // scheduler before its workers had reconnected.
  val noSpuriousFail = CLIENTS.forall(c => cl.get(c) != CFailed)

  val safety = all { doneImpliesPublished, slotsSane, singlePublish, noStrandedClient, noOversubscribe, entryLive, noSpuriousFail }
}

module schedFixed    { import scheduler(PUSH = true,  FENCED_PUBLISH = true,  REPORT_ON_RECONNECT = true,  UNPLACEABLE_TERMINAL = false).* }
// each drops one mechanism; `safety` must find the hole
module schedNoPush   { import scheduler(PUSH = false, FENCED_PUBLISH = true,  REPORT_ON_RECONNECT = true,  UNPLACEABLE_TERMINAL = false).* }
module schedNoReport { import scheduler(PUSH = true,  FENCED_PUBLISH = false, REPORT_ON_RECONNECT = false, UNPLACEABLE_TERMINAL = false).* }
// without fencing the duplicate build after a scheduler restart publishes twice
module schedNoFence  { import scheduler(PUSH = true,  FENCED_PUBLISH = false, REPORT_ON_RECONNECT = true,  UNPLACEABLE_TERMINAL = false).* }
// scheduler answers "no worker" right after a restart and drops the Want
module schedTerminal { import scheduler(PUSH = true,  FENCED_PUBLISH = true,  REPORT_ON_RECONNECT = true,  UNPLACEABLE_TERMINAL = true).* }
