// -*- mode: Bluespec; -*-
// --scheduler-order with two nodes behind envoy priorities: new streams go
// to S1 while it is up, else to S2. Streams stay where they were opened
// until closed. One derivation, two clients, two workers with one slot
// each. scheduler.qnt covers one scheduler in detail; this asks what a
// second one can break and shows the step-down rule (Elector) repairs it.

module failover {

  pure val CLIENTS: Set[str] = Set("c1", "c2")
  pure val WORKERS: Set[str] = Set("w1", "w2")
  pure val SCHEDS: Set[str] = Set("s1", "s2")
  pure val MAX_FLAPS: int = 3

  // S2 closes every stream with Restarting and refuses new ones while it
  // sees S1 healthy. Without it S2 keeps whoever it has after S1 returns.
  const STEP_DOWN: bool
  // The cache accepts the first publish of an output only.
  const FENCED: bool

  // client phase: "idle" | "queued" | "sent" (to worker cTo) | "done"
  // per scheduler, for the one drv: who it put it on and who waits
  // has: an entry exists; worker "" = queued, nobody free yet
  type SState = { has: bool, worker: str, followers: Set[str], busy: Set[str] }
  type WState = { running: bool, expecting: bool, attached: Set[str], built: bool }

  var s1up: bool
  var flaps: int
  var ss: str -> SState
  var cl: str -> str
  var cTo: str -> str
  var cOn: str -> str          // client stream bound to ("" = none)
  var wOn: str -> str          // worker session bound to ("" = none)
  var ws: str -> WState
  var published: int
  // messages carry the scheduler they belong to; a peer that has since
  // rebound drops them
  var mWant: Set[(str, str)]         // (client, sched)
  var mAssigned: Set[(str, str, str)] // (sched, client, worker)
  var mExpect: Set[(str, str)]       // (sched, worker)
  var mBuild: Set[(str, str)]        // (client, worker)
  var mResult: Set[str]              // client
  var mDone: Set[(str, str)]         // (worker, sched)
  var mRetry: Set[str]               // client: worker bounced, ask again
  var mRestart: Set[str]             // client or worker: stream closed, reopen

  pure def s0: SState = { has: false, worker: "", followers: Set(), busy: Set() }
  pure def w0: WState = { running: false, expecting: false, attached: Set(), built: false }

  def route: str = if (s1up) "s1" else "s2"
  def accepts(s: str): bool = if (s == "s1") s1up else (not(STEP_DOWN) or not(s1up))

  action init = all {
    s1up' = true, flaps' = 0,
    ss' = SCHEDS.mapBy(_ => s0),
    cl' = CLIENTS.mapBy(_ => "idle"), cTo' = CLIENTS.mapBy(_ => ""),
    cOn' = CLIENTS.mapBy(_ => ""),
    wOn' = WORKERS.mapBy(_ => "s1"),
    ws' = WORKERS.mapBy(_ => w0),
    published' = 0,
    mWant' = Set(), mAssigned' = Set(), mExpect' = Set(), mBuild' = Set(),
    mResult' = Set(), mDone' = Set(), mRetry' = Set(), mRestart' = Set(),
  }

  action keepMsgs = all {
    mWant' = mWant, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild,
    mResult' = mResult, mDone' = mDone, mRetry' = mRetry, mRestart' = mRestart,
  }
  action keepEnv = all { s1up' = s1up, flaps' = flaps }

  // ---- envoy / schedulers coming and going ----

  // S1 dies: its state is gone, its streams break.
  action s1Down: bool = all {
    s1up, flaps < MAX_FLAPS,
    s1up' = false, flaps' = flaps + 1,
    ss' = ss.set("s1", s0),
    cOn' = CLIENTS.mapBy(c => if (cOn.get(c) == "s1") "" else cOn.get(c)),
    wOn' = WORKERS.mapBy(w => if (wOn.get(w) == "s1") "" else wOn.get(w)),
    mRestart' = mRestart.union(CLIENTS.filter(c => cOn.get(c) == "s1" and cl.get(c) == "queued"))
                        .union(WORKERS.filter(w => wOn.get(w) == "s1")),
    mWant' = mWant.filter(p => p._2 != "s1"),
    mAssigned' = mAssigned.filter(p => p._1 != "s1"),
    mExpect' = mExpect.filter(p => p._1 != "s1"),
    mDone' = mDone.filter(p => p._2 != "s1"),
    mBuild' = mBuild, mResult' = mResult, mRetry' = mRetry,
    cl' = cl, cTo' = cTo, ws' = ws, published' = published,
  }

  // S1 returns empty. With STEP_DOWN S2 notices and sheds everyone.
  action s1Back: bool = all {
    not(s1up),
    s1up' = true, flaps' = flaps,
    if (STEP_DOWN) all {
      ss' = ss.set("s2", s0),
      cOn' = CLIENTS.mapBy(c => if (cOn.get(c) == "s2") "" else cOn.get(c)),
      wOn' = WORKERS.mapBy(w => if (wOn.get(w) == "s2") "" else wOn.get(w)),
      mRestart' = mRestart.union(CLIENTS.filter(c => cOn.get(c) == "s2" and cl.get(c) == "queued"))
                          .union(WORKERS.filter(w => wOn.get(w) == "s2")),
      mWant' = mWant.filter(p => p._2 != "s2"),
      mAssigned' = mAssigned.filter(p => p._1 != "s2"),
      mExpect' = mExpect.filter(p => p._1 != "s2"),
      mDone' = mDone.filter(p => p._2 != "s2"),
    } else all {
      ss' = ss, cOn' = cOn, wOn' = wOn,
      mRestart' = mRestart, mWant' = mWant, mAssigned' = mAssigned, mExpect' = mExpect, mDone' = mDone,
    },
    mBuild' = mBuild, mResult' = mResult, mRetry' = mRetry,
    cl' = cl, cTo' = cTo, ws' = ws, published' = published,
  }

  // ---- client ----

  // Open (or reopen) the Schedule stream through envoy and send Want.
  action want(c: str): bool = all {
    (cl.get(c) == "idle") or (cl.get(c) == "queued" and cOn.get(c) == "" and (mRestart.contains(c) or mRetry.contains(c))),
    accepts(route),
    cl' = cl.set(c, "queued"), cTo' = cTo,
    cOn' = cOn.set(c, route),
    mWant' = mWant.union(Set((c, route))),
    mRestart' = mRestart.exclude(Set(c)), mRetry' = mRetry.exclude(Set(c)),
    mAssigned' = mAssigned.filter(p => p._2 != c),
    mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mDone' = mDone,
    ss' = ss, wOn' = wOn, ws' = ws, published' = published, keepEnv,
  }

  // Worker bounced us while the stream is still open: Want again on it.
  action rewant(c: str): bool = all {
    mRetry.contains(c), cl.get(c) == "queued", cOn.get(c) != "",
    mWant' = mWant.union(Set((c, cOn.get(c)))),
    mRetry' = mRetry.exclude(Set(c)),
    mAssigned' = mAssigned.filter(p => p._2 != c),
    mRestart' = mRestart, mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mDone' = mDone,
    cl' = cl, cTo' = cTo, cOn' = cOn, ss' = ss, wOn' = wOn, ws' = ws, published' = published, keepEnv,
  }

  action recvAssigned(s: str, c: str, w: str): bool = all {
    mAssigned.contains((s, c, w)),
    mAssigned' = mAssigned.exclude(Set((s, c, w))),
    if (cl.get(c) == "queued" and cOn.get(c) == s) all {
      cl' = cl.set(c, "sent"), cTo' = cTo.set(c, w),
      mBuild' = mBuild.union(Set((c, w))),
    } else all { cl' = cl, cTo' = cTo, mBuild' = mBuild },
    mWant' = mWant, mExpect' = mExpect, mResult' = mResult, mDone' = mDone, mRetry' = mRetry, mRestart' = mRestart,
    cOn' = cOn, ss' = ss, wOn' = wOn, ws' = ws, published' = published, keepEnv,
  }

  action recvResult(c: str): bool = all {
    mResult.contains(c),
    mResult' = mResult.exclude(Set(c)),
    cl' = cl.set(c, "done"), cTo' = cTo.set(c, ""), cOn' = cOn.set(c, ""),
    mWant' = mWant, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mDone' = mDone, mRetry' = mRetry, mRestart' = mRestart,
    ss' = ss, wOn' = wOn, ws' = ws, published' = published, keepEnv,
  }

  // Worker said UNAVAILABLE: back to queued, will rewant / want.
  action recvBounce(c: str): bool = all {
    mRetry.contains(c), cl.get(c) == "sent",
    cl' = cl.set(c, "queued"), cTo' = cTo.set(c, ""),
    keepMsgs, cOn' = cOn, ss' = ss, wOn' = wOn, ws' = ws, published' = published, keepEnv,
  }

  // ---- scheduler ----

  def cached: bool = published > 0

  action recvWant(c: str, s: str): bool = all {
    mWant.contains((c, s)),
    mWant' = mWant.exclude(Set((c, s))),
    val st = ss.get(s)
    if (cOn.get(c) != s) all {
      ss' = ss, mAssigned' = mAssigned, mExpect' = mExpect, mResult' = mResult,
    } else if (cached) all {
      ss' = ss, mAssigned' = mAssigned, mExpect' = mExpect, mResult' = mResult.union(Set(c)),
    } else if (st.has) all {
      ss' = ss.set(s, { ...st, followers: st.followers.union(Set(c)) }),
      mAssigned' = if (st.worker != "") mAssigned.union(Set((s, c, st.worker))) else mAssigned,
      mExpect' = mExpect, mResult' = mResult,
    } else {
      val avail = WORKERS.filter(w => wOn.get(w) == s and not(st.busy.contains(w)))
      if (avail.size() == 0) all {
        ss' = ss.set(s, { ...st, has: true, worker: "", followers: Set(c) }),
        mAssigned' = mAssigned, mExpect' = mExpect, mResult' = mResult,
      } else {
        nondet w = avail.oneOf()
        all {
          ss' = ss.set(s, { has: true, worker: w, followers: Set(c), busy: st.busy.union(Set(w)) }),
          mAssigned' = mAssigned.union(Set((s, c, w))),
          mExpect' = mExpect.union(Set((s, w))),
          mResult' = mResult,
        }
      }
    },
    mBuild' = mBuild, mDone' = mDone, mRetry' = mRetry, mRestart' = mRestart,
    cl' = cl, cTo' = cTo, cOn' = cOn, wOn' = wOn, ws' = ws, published' = published, keepEnv,
  }

  // A queued entry gets a worker once one has a session and a free slot.
  action place(s: str): bool = {
    val st = ss.get(s)
    all {
      st.has, st.worker == "",
      val avail = WORKERS.filter(w => wOn.get(w) == s and not(st.busy.contains(w)))
      all {
        avail.size() > 0,
        nondet w = avail.oneOf()
        all {
          ss' = ss.set(s, { ...st, worker: w, busy: st.busy.union(Set(w)) }),
          mAssigned' = mAssigned.union(st.followers.map(c => (s, c, w))),
          mExpect' = mExpect.union(Set((s, w))),
        }
      },
      mWant' = mWant, mBuild' = mBuild, mResult' = mResult, mDone' = mDone, mRetry' = mRetry, mRestart' = mRestart,
      cl' = cl, cTo' = cTo, cOn' = cOn, wOn' = wOn, ws' = ws, published' = published, keepEnv,
    }
  }

  action recvDone(w: str, s: str): bool = all {
    mDone.contains((w, s)),
    mDone' = mDone.exclude(Set((w, s))),
    val st = ss.get(s)
    ss' = ss.set(s,
      if (st.has and st.worker == w) { ...s0, busy: st.busy.exclude(Set(w)) }
      else { ...st, busy: st.busy.exclude(Set(w)) }),
    mWant' = mWant, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mRetry' = mRetry, mRestart' = mRestart,
    cl' = cl, cTo' = cTo, cOn' = cOn, wOn' = wOn, ws' = ws, published' = published, keepEnv,
  }

  // ---- worker ----

  // (Re)open the session through envoy; Hello carries what is running, and
  // the scheduler adopts it unless it already put the drv elsewhere.
  action hello(w: str): bool = all {
    wOn.get(w) == "", accepts(route),
    wOn' = wOn.set(w, route),
    mRestart' = mRestart.exclude(Set(w)),
    val s = route
    val st = ss.get(s)
    val live = ws.get(w).running or ws.get(w).expecting
    all {
      ss' = ss.set(s,
        if (not(live)) st
        else if (st.has and st.worker != "" and st.worker != w) { ...st, busy: st.busy.union(Set(w)) }
        else { has: true, worker: w, followers: if (st.has) st.followers else Set(), busy: st.busy.union(Set(w)) }),
      // followers of a queued entry that just got adopted learn where to go
      mAssigned' =
        if (live and st.has and st.worker == "") mAssigned.union(st.followers.map(c => (s, c, w)))
        else mAssigned,
    },
    mWant' = mWant, mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mDone' = mDone, mRetry' = mRetry,
    cl' = cl, cTo' = cTo, cOn' = cOn, ws' = ws, published' = published, keepEnv,
  }

  action recvExpect(s: str, w: str): bool = all {
    mExpect.contains((s, w)),
    mExpect' = mExpect.exclude(Set((s, w))),
    ws' = ws.set(w, if (wOn.get(w) == s and not(ws.get(w).built) and not(cached)) { ...ws.get(w), expecting: true } else ws.get(w)),
    // already have it: report straight back so the slot frees
    mDone' = if (wOn.get(w) == s and (ws.get(w).built or cached)) mDone.union(Set((w, s))) else mDone,
    mWant' = mWant, mAssigned' = mAssigned, mBuild' = mBuild, mResult' = mResult, mRetry' = mRetry, mRestart' = mRestart,
    cl' = cl, cTo' = cTo, cOn' = cOn, ss' = ss, wOn' = wOn, published' = published, keepEnv,
  }

  action recvBuild(c: str, w: str): bool = all {
    mBuild.contains((c, w)),
    mBuild' = mBuild.exclude(Set((c, w))),
    val st = ws.get(w)
    if (not(cl.get(c) == "sent" and cTo.get(c) == w)) all {
      ws' = ws, mResult' = mResult, mRetry' = mRetry,
    } else if (st.built or cached) all {
      ws' = ws, mResult' = mResult.union(Set(c)), mRetry' = mRetry,
    } else if (st.running or st.expecting) all {
      ws' = ws.set(w, { ...st, running: true, attached: st.attached.union(Set(c)) }),
      mResult' = mResult, mRetry' = mRetry,
    } else all {
      ws' = ws, mResult' = mResult, mRetry' = mRetry.union(Set(c)),
    },
    mWant' = mWant, mAssigned' = mAssigned, mExpect' = mExpect, mDone' = mDone, mRestart' = mRestart,
    cl' = cl, cTo' = cTo, cOn' = cOn, ss' = ss, wOn' = wOn, published' = published, keepEnv,
  }

  // Expect that no client followed up on expires.
  action expire(w: str): bool = all {
    ws.get(w).expecting and not(ws.get(w).running),
    ws' = ws.set(w, { ...ws.get(w), expecting: false }),
    mDone' = if (wOn.get(w) != "") mDone.union(Set((w, wOn.get(w)))) else mDone,
    mWant' = mWant, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mResult' = mResult, mRetry' = mRetry, mRestart' = mRestart,
    cl' = cl, cTo' = cTo, cOn' = cOn, ss' = ss, wOn' = wOn, published' = published, keepEnv,
  }

  action finish(w: str): bool = all {
    ws.get(w).running,
    published' = if (not(FENCED) or published == 0) published + 1 else published,
    ws' = ws.set(w, { running: false, expecting: false, built: true, attached: Set() }),
    mResult' = mResult.union(ws.get(w).attached),
    mDone' = if (wOn.get(w) != "") mDone.union(Set((w, wOn.get(w)))) else mDone,
    mWant' = mWant, mAssigned' = mAssigned, mExpect' = mExpect, mBuild' = mBuild, mRetry' = mRetry, mRestart' = mRestart,
    cl' = cl, cTo' = cTo, cOn' = cOn, ss' = ss, wOn' = wOn, keepEnv,
  }

  action step = {
    nondet c = CLIENTS.oneOf()
    nondet w = WORKERS.oneOf()
    nondet s = SCHEDS.oneOf()
    any {
      s1Down, s1Back,
      want(c), rewant(c), recvAssigned(s, c, w), recvResult(c), recvBounce(c),
      recvWant(c, s), place(s), recvDone(w, s),
      hello(w), recvExpect(s, w), recvBuild(c, w), expire(w), finish(w),
    }
  }

  // ---- properties ----

  val singlePublish = published <= 1
  val doneImpliesPublished = CLIENTS.forall(c => cl.get(c) == "done" implies published > 0)

  // Every waiting client has something in flight or is known to a scheduler
  // that can still act for it.
  val noStrandedClient = CLIENTS.forall(c =>
    cl.get(c) == "idle" or cl.get(c) == "done"
    or mWant.contains((c, cOn.get(c))) or mResult.contains(c) or mRetry.contains(c) or mRestart.contains(c)
    or mAssigned.exists(p => p._2 == c and p._1 == cOn.get(c)) or mBuild.exists(p => p._1 == c)
    or WORKERS.exists(w => ws.get(w).attached.contains(c))
    or (cOn.get(c) != "" and ss.get(cOn.get(c)).has and ss.get(cOn.get(c)).followers.contains(c)))

  // Once S1 is back nobody is left on S2: otherwise new clients (on S1) and
  // old workers (on S2) never meet.
  val converged = (STEP_DOWN and s1up) implies
    (CLIENTS.forall(c => cOn.get(c) != "s2") and WORKERS.forall(w => wOn.get(w) != "s2"))

  // A client parked on a scheduler that has no worker and will not get one.
  val noOrphanQueue = SCHEDS.forall(s =>
    (ss.get(s).has and ss.get(s).worker == "")
      implies WORKERS.exists(w => wOn.get(w) == s or (wOn.get(w) == "" and (if (s == "s1") s1up else accepts("s2") and not(s1up)))))

  val oneBuilder = WORKERS.filter(w => ws.get(w).running).size() <= 1

  val safety = all { singlePublish, doneImpliesPublished, noStrandedClient, converged, noOrphanQueue }
}

// Step-down: safe, and once S1 is back nobody is left on S2. A double build
// in the hand-over window is possible in principle (client re-Wants on S1
// before the worker already running it says Hello there); Expect gating and
// Hello adoption make it rare, the cache's publish fence makes it harmless.
module foFixed      { import failover(STEP_DOWN = true,  FENCED = true).* }
// S2 keeps its peers after S1 returns: a client sits on a scheduler that
// will never see a worker again (violates noOrphanQueue in 4 steps).
module foNoStepDown { import failover(STEP_DOWN = false, FENCED = true).* }
