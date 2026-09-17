// -*- mode: Bluespec; -*-
// Worker daemon <-> `niks3 push --stdin`: requests are lines of paths, the
// child acks each (request, path). The daemon must route acks so that every
// request completes even when concurrent requests share paths.
module push {
  const KEY_BY_ID: bool

  type Req = int
  type Path = str
  val REQS: Set[Req] = Set(1, 2, 3)
  val PATHS: Set[Path] = Set("a", "b")

  var sent: Req -> Set[Path]          // paths of each issued request
  var left: Req -> int                // acks the daemon still expects
  var inflight: Set[(Req, Path)]      // acks the child still owes
  var byPath: Path -> Req             // daemon routing table when not KEY_BY_ID

  action init = all {
    sent' = Map(), left' = Map(), inflight' = Set(), byPath' = Map(),
  }

  action send(r: Req, ps: Set[Path]): bool = all {
    not(sent.keys().contains(r)), ps != Set(),
    sent' = sent.put(r, ps),
    left' = left.put(r, ps.size()),
    inflight' = inflight.union(ps.map(p => (r, p))),
    byPath' = ps.fold(byPath, (m, p) => m.put(p, r)),
  }

  action ack(r: Req, p: Path): bool = all {
    inflight.contains((r, p)),
    inflight' = inflight.exclude(Set((r, p))),
    sent' = sent,
    if (KEY_BY_ID) all {
      left' = left.set(r, left.get(r) - 1),
      byPath' = byPath,
    } else if (byPath.keys().contains(p)) all {
      left' = left.set(byPath.get(p), left.get(byPath.get(p)) - 1),
      byPath' = byPath.keys().exclude(Set(p)).mapBy(k => byPath.get(k)),
    } else all {
      left' = left, byPath' = byPath,
    },
  }

  action step = any {
    nondet r = REQS.oneOf()
    nondet ps = PATHS.powerset().exclude(Set(Set())).oneOf()
    send(r, ps),
    all {
      inflight != Set(),
      nondet a = inflight.oneOf()
      ack(a._1, a._2),
    },
  }

  // Once the child has answered everything, no request may still be waiting.
  val allAnswered = inflight == Set() implies left.keys().forall(r => left.get(r) == 0)
  val noOverCount = left.keys().forall(r => left.get(r) >= 0)
  val safety = allAnswered and noOverCount
}

module pushFixed { import push(KEY_BY_ID = true).* }
module pushByPath { import push(KEY_BY_ID = false).* }
