// -*- mode: Bluespec; -*-
// The build hook against a balanced farm: each RPC may land on a different
// worker and any worker may restart between RPCs (src/grpc-store.cc
// buildDerivationNative, src/nix-grpc-daemon.cc QueryValidPaths,
// AddMultipleToStore, farmBuild). One derivation, one input, one output.

module hook {

  type Phase = Query | Upload | Build | UploadDrv | Fetch | Done | Failed

  pure val WORKERS: Set[str] = Set("w1", "w2")
  pure val IN: str = "in"
  pure val DRV: str = "drv"
  pure val OUT: str = "out"
  pure val MAX_RESTARTS: int = 2
  pure val RETRIES: int = 3

  const PUBLISH_BEFORE_ACK: bool   // daemon: QueryValidPaths/AddMultipleToStore push first
  const RETRY_ALL: bool            // client: every step retried elsewhere, not just Build
  const SUBSTITUTE_REFS: bool      // daemon: AddMultipleToStore fetches missing references from the cache
  const SUBSTITUTE_DRV: bool       // daemon: BuildDerivation fetches a missing drv closure from the cache
  def retried(p: Phase): bool = RETRY_ALL or p == Build or p == UploadDrv
  def publish(c: Set[str], ps: Set[str]): Set[str] = if (PUBLISH_BEFORE_ACK) c.union(ps) else c

  var local: str -> Set[str]   // worker stores, survive restarts
  var cache: Set[str]          // niks3
  var phase: Phase
  var toUpload: Set[str]       // what Query told the client is missing
  var retries: int
  var restarts: int
  var drvUploaded: bool         // client already answered one NOT_FOUND with an upload

  action init = {
    nondet pre = WORKERS.powerset().oneOf()   // workers that happen to hold IN already
    all {
      local' = WORKERS.mapBy(w => if (pre.contains(w)) Set(IN) else Set()),
      cache' = Set(),
      phase' = Query,
      toUpload' = Set(),
      retries' = 0,
      restarts' = 0,
      drvUploaded' = false,
    }
  }

  // QueryValidPaths({IN, DRV}) at w
  action query(w: str): bool = all {
    phase == Query,
    val have = local.get(w).intersect(Set(IN, DRV))
    all {
      cache' = publish(cache, have),
      toUpload' = Set(IN, DRV).exclude(have),
      phase' = Upload,
      local' = local, retries' = retries, restarts' = restarts, drvUploaded' = drvUploaded,
    }
  }

  // AddMultipleToStore(toUpload) at w. DRV refers to IN, and nix only registers a path whose references are valid.
  action upload(w: str): bool = all {
    phase == Upload,
    val have = local.get(w).union(toUpload)
    val fetched = if (SUBSTITUTE_REFS and not(have.contains(IN)) and cache.contains(IN)) Set(IN) else Set()
    val refsOk = not(toUpload.contains(DRV)) or have.union(fetched).contains(IN)
    all {
      if (refsOk) all {
        local' = local.setBy(w, s => s.union(toUpload).union(fetched)),
        cache' = publish(cache, toUpload),
        phase' = Build,
        retries' = retries,
      } else all {
        local' = local, cache' = cache,
        phase' = Failed,
        retries' = retries,
      },
      toUpload' = toUpload, restarts' = restarts, drvUploaded' = drvUploaded,
    }
  }

  // BuildDerivation at w. A drv the worker lacks is fetched from the cache,
  // else NOT_FOUND: the client uploads the drv closure and calls again, and
  // that second call may land on any worker.
  action build(w: str): bool = all {
    phase == Build,
    val fetched = if (SUBSTITUTE_DRV and not(local.get(w).contains(DRV)) and cache.contains(DRV)) Set(DRV) else Set()
    val st = local.get(w).union(fetched)
    val haveDrv = st.contains(DRV)
    val inputOk = st.contains(IN) or cache.contains(IN)
    all {
      local' = local.set(w, if (haveDrv and inputOk) st.union(Set(IN, OUT)) else st),
      // outputs are published before BuildDerivation returns
      cache' = cache.union(if (haveDrv and inputOk) Set(OUT) else Set()),
      phase' = if (not(haveDrv)) (if (drvUploaded) Failed else UploadDrv) else if (inputOk) Fetch else Failed,
      toUpload' = toUpload, retries' = retries, restarts' = restarts, drvUploaded' = drvUploaded,
    }
  }

  // AddMultipleToStore({DRV, IN} minus what w has) after NOT_FOUND, then BuildDerivation again.
  action uploadDrv(w: str): bool = all {
    phase == UploadDrv,
    val up = Set(DRV, IN).exclude(local.get(w))
    all {
      local' = local.setBy(w, s => s.union(up)),
      cache' = publish(cache, up),
      phase' = Build,
      drvUploaded' = true,
      toUpload' = toUpload, retries' = retries, restarts' = restarts,
    }
  }

  // FetchNars(OUT) at w: serves local or substitutes from cache
  action fetch(w: str): bool = all {
    phase == Fetch,
    phase' = if (local.get(w).contains(OUT) or cache.contains(OUT)) Done else Failed,
    local' = local, cache' = cache, toUpload' = toUpload, retries' = retries, restarts' = restarts, drvUploaded' = drvUploaded,
  }

  // w restarts while serving the current RPC. A cut upload may have landed
  // some paths in w's store, unpublished.
  action restart(w: str): bool = {
    nondet landed = toUpload.powerset().oneOf()
    all {
      restarts < MAX_RESTARTS,
      phase != Done, phase != Failed,
      restarts' = restarts + 1,
      retries' = retries + 1,
      phase' = if (retried(phase) and retries + 1 <= RETRIES) (if (phase == UploadDrv) Build else phase) else Failed,
      local' = if (phase == Upload) local.setBy(w, s => s.union(landed)) else local,
      cache' = cache, toUpload' = toUpload, drvUploaded' = drvUploaded,
    }
  }

  action step = {
    nondet w = WORKERS.oneOf()
    any { query(w), upload(w), build(w), uploadDrv(w), fetch(w), restart(w) }
  }

  val noSpuriousFailure = (phase == Failed) implies (retries > RETRIES)
  val doneMeansFetchable = (phase == Done) implies cache.contains(OUT)
  val safety = all { noSpuriousFailure, doneMeansFetchable }
}

module hookFixed { import hook(PUBLISH_BEFORE_ACK = true, RETRY_ALL = true, SUBSTITUTE_REFS = true, SUBSTITUTE_DRV = true).* }
// the "input not substitutable" trace
module hookNoPublish { import hook(PUBLISH_BEFORE_ACK = false, RETRY_ALL = true, SUBSTITUTE_REFS = true, SUBSTITUTE_DRV = true).* }
// the "AddMultipleToStore: Cancelling all calls" trace
module hookBuildRetryOnly { import hook(PUBLISH_BEFORE_ACK = true, RETRY_ALL = false, SUBSTITUTE_REFS = true, SUBSTITUTE_DRV = true).* }
// the "AddMultipleToStore: path ... is not valid" trace
module hookNoSubstituteRefs { import hook(PUBLISH_BEFORE_ACK = true, RETRY_ALL = true, SUBSTITUTE_REFS = false, SUBSTITUTE_DRV = true).* }

// BuildDerivation after the NOT_FOUND upload lands on a worker other than the one uploaded to.
module hookNoSubstituteDrv { import hook(PUBLISH_BEFORE_ACK = true, RETRY_ALL = true, SUBSTITUTE_REFS = true, SUBSTITUTE_DRV = false).* }
