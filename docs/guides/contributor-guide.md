# casehub-ras — Contributor Guide

> Platform internals for RAS contributors — architecture, full module details, runtime mechanics, and extension points.

**GitHub:** [casehubio/casehub-ras](https://github.com/casehubio/casehub-ras)

---

## Internal Architecture

### Routing Model — Definition-Driven (Model B)

The engine owns situation routing. Ganglia evaluate — they do not choose which situation an event belongs to. `SituationDefinition.eventTypes` is the routing key; `ChainMode` identifies participating ganglia; `Ganglion.handledEventTypes()` is a capability declaration for startup validation only. A situation instance is identified by the tuple `(situationId, correlationKey, tenancyId)`. `correlationKey` defaults to `CloudEvent.getSubject()` or `"_singleton"` when null.

### RasEngine (Entry Point)

`@ApplicationScoped` CDI bean. Observes `CloudEvent` CDI async events via `@ObservesAsync`. For each event: extracts `tenancyid` extension (skips events without it), looks up matching `SituationRegistration`s by event type, extracts correlation key, delegates to `SituationEvaluator`.

### SituationEvaluator — Two-Phase Processing

Per event:
1. **Phase 1 — Detect** (never retried): calls `ganglion.detect()` for all relevant ganglia.
2. **Phase 2 — Apply + persist** (retried on `SituationConflictException` up to `ras.evaluator.max-conflict-retries=3`): applies detections to context, evaluates trigger policy, executes decision.

Decisions: `TRIGGER` fires `CaseTrigger` then removes the situation; `TRIGGER_AND_CONTINUE` fires `CaseTrigger` but keeps the situation; `CONTINUE_ACCUMULATING` optionally compacts then saves; `DISCARD` removes; `RESOLVE` removes without creating a case.

Handles event reorder buffers (TreeMap-based, drains when watermark advances past `eventBufferDelay`) and correlation window expiry.

### Clustered Conflict Handling

`SituationEvaluator.processEvent()` is two-phase: Phase 1 detects once (ganglia mutate internal state), Phase 2 retries read-modify-write on `SituationConflictException`. `JpaSituationStore` uses two-layer conflict detection: application-level `storeVersion` comparison (non-overlapping transactions) + Hibernate `@Version` OLE/constraint violation (overlapping transactions). Max retries configurable via `ras.evaluator.max-conflict-retries` (default 3). `InMemorySituationStore` is unaffected — per-key `synchronized` locks prevent concurrent access within a single JVM.

### Duplicate Trigger Prevention

TRIGGER path uses `SituationStore.tryClaimTrigger()` for exactly-once trigger execution across clustered JVMs. `tryClaimTrigger` atomically stamps `lastTriggered` and increments `triggerCount` alongside the `policyTriggered` CAS — trigger metadata is store-managed, not written by `save()`. Bifurcated claim path for TRIGGER: new entities use save-before-claim, existing entities use claim-before-save. TRIGGER_AND_CONTINUE uses save-first flow (no bifurcation). Entity removal is deferred after successful trigger — the `policyTriggered=true` entity guards against duplicate triggers from retrying losers, cleaned up by `SituationExpiryJob` after a configurable guard period (`ras.evaluator.trigger-guard-period`, default PT1M). On trigger failure, `resetTriggerClaim()` clears the flag for retry.

### Ganglion Error Isolation

`SituationEvaluator.runDetection()` wraps each `ganglion.detect()` call in a per-ganglion try-catch. One ganglion's failure (storage error, rule engine crash) is logged and skipped — remaining ganglia still evaluate and produce partial results.

### Persistent Situation Compaction

For persistent situations (`correlationWindow = null`), `SituationEvaluator` calls `Ganglion.compact()` on each referenced ganglion after every `CONTINUE_ACCUMULATING` decision. Windowed situations skip compaction.

### Dynamic Situation Registration

`SituationDefinitionRegistry` supports runtime registration and deregistration via `register(SituationRegistration)` and `deregister(String situationId)`. Thread-safe via copy-on-write `RegistrySnapshot` — one `volatile` swap per mutation, lock-free reads on `findByEventType()`. Ganglia remain static (CDI beans at startup); only situation definitions are dynamic.

For persistent situations (`correlationWindow=null`), consuming apps must call `SituationStore.removeAllForSituation()` AND `GanglionStateStore.removeForSituation()` before deregistering to avoid orphaned entries.

---

## Full Module Details

### api/ — `casehub-ras-api`

Core SPIs and domain types: `Ganglion`, `SituationStore`, `GanglionStateStore`, `CaseTrigger`, `RasTriggerPolicy`, `CaseInputContributor`, `OrphanedResourceCleaner`, `EventFilter`, `CorrelationKeyExtractor`, `SituationDefinitionProvider`, `SituationQueryService`, `SituationEventRetention`. Records: `SituationDefinition`, `SituationContext`, `DetectionResult`, `CaseTriggerConfig`, `GanglionState`, `GanglionStateKey`, `SituationRegistration`, `ActiveSituation`, `SituationChangeEvent`, `SituationEvent`, `TrendResult`, `TenantHealth`, `SituationSummary`. Sealed types: `ChainMode` (7 variants), `TriggerMode` (FireOnce, Repeating), `TriggerAction` (CreateCase, NotifyOnly), `GanglionDescriptor` (NaiveBayes, ExpressionRules). Base class: `JavaSwitchGanglion`. Depends on CloudEvents SDK, `casehub-platform-api`. No CDI. Publishes test-jar with `AbstractGanglionContractTest`, `AbstractGanglionStateStoreContractTest`, `AbstractSituationQueryServiceContractTest`.

### runtime/ — `casehub-ras`

CDI runtime: `RasEngine` (CloudEvent observer), `SituationEvaluator` (two-phase detection + OCC retry), `SituationDefinitionRegistry` (three-phase constructor: descriptor ganglia, CDI ganglia, situation registrations), `DefaultRasTriggerPolicy`, `DefaultCaseTrigger`, `YamlSituationDefinitionProvider`, `NaiveBayesGanglion`, `ExpressionRulesGanglion`, `EvidenceExtractingGanglion`, `DefaultSituationSource`, `RasEndpointRegistration`, `RasMetrics`, `InMemoryGanglionStateStore` (`@DefaultBean`), `CloudEventExpressionContext`, `SituationContextExpressionContext`, `ExpressionFeatureExtractor`, `JqResultUnwrapper`. Scheduled expiry + buffer flush jobs. Micrometer metrics (optional, via `Instance<MeterRegistry>`). `casehub-platform-expression` at test scope only — deployers add it to classpath when expressions are needed. Quarkus extension.

### persistence-memory/ — `casehub-ras-persistence-memory`

`InMemorySituationStore` (`@Alternative @Priority(100)`, ConcurrentHashMap-backed). `InMemorySituationQueryService` (`@Alternative @Priority(100)`, CDI `@ObservesAsync` event capture). Dev/test only. Zero config.

### persistence-jpa/ — `casehub-ras-persistence-jpa`

`JpaSituationStore` — JPA-backed with dual-layer OCC (application-level `storeVersion` + JPA `@Version`). `SituationEntity` (table `ras_situation`, JSONB detections). `JpaGanglionStateStore` — `GanglionStateEntity` with JSONB state + `@Version` optimistic locking. `JpaSituationQueryService` — implements `SituationQueryService` + `SituationEventRetention`. `SituationEventRecorder` — CDI observer, `@Transactional`, best-effort event log capture. Implements `OrphanedResourceCleaner` for SQL join-based orphan cleanup. Flyway V1-V2. Consumers add `classpath:db/ras/migration` to `quarkus.flyway.locations`.

### ras-drools/ — `casehub-ras-drools`

`DroolsGanglion` — Drools CEP stream-mode engine. Builds `KieBase` with `EventProcessingOption.STREAM` from DRL rules. Two session modes: `LONG_LIVED` (stateful, keyed by situation/correlation/tenancy, persisted in `DroolsSessionStore`, lazily invalidated on rule reload via generation counter) and `EPHEMERAL` (new session per event). Two clock modes: `PSEUDO` (event-time driven) and `REALTIME`. `DroolsObjectExtractor` SPI for domain-specific fact insertion. `ResultCollectionStrategy`: `HIGHEST_CONFIDENCE`, `FIRST_MATCH`, `LAST_WINS`, `ACCUMULATE`. Uses classic kie-api.

### drools-reliability/ — `casehub-ras-drools-reliability`

`ReliableDroolsSessionStore` — `@ApplicationScoped` `DroolsSessionStore` backed by `drools-reliability-h2mvstore`. ConcurrentHashMap hot cache with generation-based eviction. Corrupt store auto-recovery. `STORES_ONLY` persistence strategy with `AFTER_FIRE` safepoints. Implements `OrphanedResourceCleaner` for orphaned session cleanup. `DroolsReliabilityMetrics` (Micrometer) and `ReliableDroolsSessionStoreHealthCheck` (`@Readiness`). Experimental.

### ras-llm/ — `casehub-ras-llm`

POM-only placeholder. Intended: LLM-based ganglion via `casehub-platform-agent-api` for narrative and ambiguous signal detection. No source yet.

### testing/ — `casehub-ras-testing`

`MockGanglion`, `MockCaseTrigger`, `FixedDetectionResult`. Test scope only.

---

## Internal SPIs

### SituationStore — situation persistence

```java
interface SituationStore {
    Optional<SituationContext> find(String situationId, String correlationKey, String tenancyId);
    SituationContext save(SituationContext context);
    void remove(String situationId, String correlationKey, String tenancyId);
    int removeExpired(Instant cutoff);
    void removeAllForSituation(String situationId);
    default boolean tryClaimTrigger(...) { ... }
    default void resetTriggerClaim(...) { }
    default int removeTriggeredBefore(Instant triggerCutoff) { ... }
    default List<SituationContext> findActive(String tenancyId) { ... }
}
```

### GanglionStateStore — ganglion computation state

```java
interface GanglionStateStore {
    Optional<GanglionState> load(GanglionStateKey key);
    void save(GanglionStateKey key, GanglionState state);
    void remove(GanglionStateKey key);
    void removeForSituation(String situationId);
}
```

Pluggable persistence for numeric accumulation. `GanglionStateKey` is a 4-tuple `(ganglionId, situationId, correlationKey, tenancyId)`. `GanglionState` carries `double[] values` + `OptionalLong storeVersion` for OCC. `InMemoryGanglionStateStore` (`@DefaultBean` in runtime/) is zero-config default. `JpaGanglionStateStore` in persistence-jpa/ wins by CDI priority when on classpath.

### OrphanedResourceCleaner — derived resource cleanup

```java
interface OrphanedResourceCleaner {
    String cleanerType();
    int removeOrphaned();
}
```

Generic SPI for cleaning up derived resources whose parent situation no longer exists. Discovered via CDI `Instance<OrphanedResourceCleaner>` in `SituationExpiryJob`. Implementations: `JpaGanglionStateStore` (SQL join-based), `ReliableDroolsSessionStore` (`SituationStore.find()` per key). Metric: `ras.expiry.orphans_cleaned` counter tagged by `cleaner_type`.

### CaseTrigger — case creation bridge

```java
interface CaseTrigger {
    UUID fire(CaseTriggerConfig config, SituationContext context);
}
```

`DefaultCaseTrigger` resolves the case definition from `CaseHubRuntime` by namespace/name/version and starts it with input data containing all detections.

### RasTriggerPolicy — trigger evaluation

```java
interface RasTriggerPolicy {
    PolicyDecision evaluate(SituationContext context, SituationDefinition definition);
}
```

`DefaultRasTriggerPolicy` maps `ChainMode` evaluation + `TriggerMode` to `TriggerDecision`.

---

## Core Types Reference

| Type | Purpose |
|------|---------|
| `CloudEvent` | Input — from `io.cloudevents:cloudevents-core` via `casehub-platform-api` |
| `DetectionResult` | Ganglion output — `ganglionId`, `confidence` (0.0–1.0), `signal`, `evidence` |
| `DetectionSignal` | Signal strength — `NOISE`, `ANTI`, `WEAK`, `DETECTED` (ascending) |
| `TimestampedDetection` | Wraps `DetectionResult` + `Instant eventTime` |
| `SituationContext` | Accumulated state — situationId, correlationKey, tenancyId, detections, storeVersion, lastTriggered, triggerCount |
| `SituationConflictException` | Thrown by `SituationStore.save()` on concurrent modification — evaluator catches and retries |
| `GanglionStateConflictException` | Thrown by `GanglionStateStore.save()` on concurrent modification |
| `SituationChangeEvent` | CDI event — tenancyId, situationId, correlationKey, ChangeType, SituationContext |
| `SituationEvent` | Event log record for historical queries |
| `TrendResult` | Trend query result — currentCount, baselineCount, TrendDirection |
| `TenantHealth` | Per-tenant aggregate — totalEvents, List<SituationSummary> |
| `ActiveSituation` | Read-only projection for external consumers |

---

## Depended On By

| Repo | What it uses |
|------|-------------|
| Application-tier repos that need situational awareness | `casehub-ras-api` for Ganglion SPI; runtime + persistence module for CDI activation |

---

## Current State

- All modules on main: API, runtime, ras-drools, drools-reliability, persistence-memory, persistence-jpa, testing. `ras-llm` scaffolded (POM only, no source).
- `NaiveBayesGanglion` built into runtime with full incremental Bayesian classification.
- `ExpressionRulesGanglion` for ordered boolean condition-to-signal rules.
- `DroolsGanglion` supports CEP stream mode with long-lived sessions, hot rule reload, and classic kie-api.
- `ReliableDroolsSessionStore` backed by H2MVStore with Micrometer metrics and MicroProfile health check.
- `GanglionStateStore` SPI with InMemory and JPA implementations (full OCC).
- JPA persistence with dual-layer OCC (application + JPA `@Version`) for both situations and ganglion state.
- 7 ChainMode variants: And, Or, Threshold, Sequence, Count, Streak, Rate.
- Trigger lifecycle: `TriggerDecision` (5 outcomes), `TriggerMode` (FireOnce/Repeating with cooldown), `TriggerAction` (CreateCase/NotifyOnly).
- `CaseInputContributor` SPI for domain-specific case seeding at trigger time.
- YAML-based situation definitions with expression support, ganglion descriptors, and situation templates.
- `SituationQueryService` SPI for historical situation data with trend and health queries.
- Dynamic situation registration/deregistration at runtime.
- API module publishes test-jars with contract tests for Ganglion, GanglionStateStore, and SituationQueryService.

---

## Design Documents

- Original: `docs/superpowers/specs/2026-06-12-casehub-ras-design.md`
- Epic 1 API: `docs/superpowers/specs/2026-06-18-epic1-core-ras-api-design.md`
- Epic 2 Runtime: `docs/superpowers/specs/2026-06-25-epic2-ras-runtime-design.md`
- Epic 3 JavaSwitchGanglion + NaiveBayesGanglion: `docs/superpowers/specs/2026-06-26-epic3-java-switch-naive-bayes-ganglion-design.md`
- Epic 4 DroolsGanglion: `docs/superpowers/specs/2026-06-21-epic4-drools-ganglion-design.md`
- Result collection + test gaps: `docs/superpowers/specs/2026-06-22-drools-result-collection-and-test-gaps.md`
- DroolsGanglion hot reload: `docs/superpowers/specs/2026-06-26-drools-hot-reload-design.md`
- Event reorder buffer: `docs/superpowers/specs/2026-06-27-event-reorder-buffer-design.md`
- JPA SituationStore: `docs/superpowers/specs/2026-06-28-jpa-situation-store-design.md`
- Clustered retry logic: `docs/superpowers/specs/2026-06-29-clustered-retry-logic-design.md`
- Trigger lifecycle + situation query: `docs/superpowers/specs/2026-06-30-trigger-lifecycle-and-situation-query-design.md`
- Service lifecycle RAS integration: `docs/superpowers/specs/2026-07-07-service-lifecycle-ras-integration-design.md`
- DroolsSessionStore hardening: `docs/superpowers/specs/2026-07-09-drools-session-store-hardening-design.md`
- RAS runtime metrics: `docs/superpowers/specs/2026-07-12-ras-runtime-metrics-design.md`
- GanglionStateStore: `docs/superpowers/specs/2026-07-13-ganglion-state-store-design.md`
- DroolsSessionStore orphan cleanup: `docs/superpowers/specs/2026-07-17-drools-session-store-orphan-cleanup-design.md`
- ExpressionEvaluator integration: `docs/superpowers/specs/2026-07-17-expression-evaluator-integration-design.md`
- JQ Map context + NaiveBayes expressions: `docs/superpowers/specs/2026-07-20-jq-map-context-naivebayes-expressions-design.md`
- Evidence templates + expression-rule ganglion: `docs/superpowers/specs/2026-07-21-evidence-templates-expression-rules-design.md`
- Per-decision-path evidence templates: `docs/superpowers/specs/2026-07-22-per-decision-path-evidence-templates-design.md`
- Retire reactive (Mutiny): `docs/superpowers/specs/2026-07-23-retire-reactive-design.md`
- Dynamic confidence expressions: `docs/superpowers/specs/2026-07-23-dynamic-confidence-expressions-design.md`
- Passive observation query service: `docs/superpowers/specs/2026-07-29-passive-observation-query-service-design.md`

---

## Key Rules

- `testing/` is never compile or runtime scope — test only
- Ganglion implementations activate by classpath presence
- `LlmGanglion` always runs async on slow path — never blocks fast detection path
- All `SituationContext` is tenancy-scoped — no cross-tenant situation accumulation
- Platform stream modules have NO dependency on `casehub-ras-api` — they fire `CloudEvent` from `casehub-platform-api`
- `casehub-ras-api` depends on `casehub-platform-api` (for `CloudEvent` type) — correct direction: integration to foundation
- casehub-ras never imports Kafka, AMQP, Camel, or any transport library
- `JavaSwitchGanglion` lives in api/ (abstract extension point, zero deps) — `NaiveBayesGanglion` lives in runtime/ (concrete implementation with internal state)
