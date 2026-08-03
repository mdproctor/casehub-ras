# casehub-ras — Consumer Guide

> Situational awareness and reactive case creation — observe CloudEvents, detect patterns via pluggable ganglia, trigger cases when thresholds are crossed.

**GitHub:** [casehubio/casehub-ras](https://github.com/casehubio/casehub-ras)
**Tier:** Foundation

---

## Purpose

Reticular Activating System — observes CloudEvent CDI async events produced by platform stream modules (Kafka, AMQP, webhook, Camel), routes them to pluggable detection strategies (Ganglia), correlates composite events across time windows, and triggers case creation via casehub-engine when a situation threshold is crossed.

The biological metaphor is deliberate: a ganglion is a neural cluster that detects a specific signal pattern. Multiple ganglia compose via `ChainMode` (AND, OR, threshold, sequence, count, streak, rate) to detect complex situations from simple parts.

---

## Module Structure

| Module | artifactId | When to use |
|--------|-----------|-------------|
| `api/` | `casehub-ras-api` | Always — core SPIs, domain types, `JavaSwitchGanglion` base class. No CDI. |
| `runtime/` | `casehub-ras` | Always — CDI runtime, `RasEngine`, `NaiveBayesGanglion`, `ExpressionRulesGanglion`, YAML definitions. Quarkus extension. |
| `persistence-memory/` | `casehub-ras-persistence-memory` | Dev/test — `@Alternative @Priority(100)`, ConcurrentHashMap-backed. Zero config. |
| `persistence-jpa/` | `casehub-ras-persistence-jpa` | Production — JPA-backed with dual-layer OCC. Add `classpath:db/ras/migration` to `quarkus.flyway.locations`. |
| `ras-drools/` | `casehub-ras-drools` | Optional — Drools CEP stream-mode ganglion with long-lived/ephemeral sessions. |
| `drools-reliability/` | `casehub-ras-drools-reliability` | Optional — persistent `DroolsSessionStore` backed by H2MVStore. Experimental. |
| `ras-llm/` | `casehub-ras-llm` | Optional — LLM-based ganglion (POM placeholder, no source yet). |
| `testing/` | `casehub-ras-testing` | Test scope only — `MockGanglion`, `MockCaseTrigger`, `FixedDetectionResult`. |

---

## Key Consumer APIs and SPIs

### Ganglion — detection strategy

The central SPI. Each ganglion handles specific CloudEvent types and produces a `DetectionResult` per event.

```java
interface Ganglion {
    String ganglionId();
    Set<String> handledEventTypes();
    DetectionResult detect(CloudEvent event, SituationContext context);
    default SituationContext compact(SituationContext context) { ... }
    default void close(String situationId, String correlationKey, String tenancyId) { }
}
```

Three built-in implementations:
- **`JavaSwitchGanglion`** (api/) — abstract base for pure-Java detection. Subclass and override `evaluate()`. Preferred path for simple stateless detection.
- **`NaiveBayesGanglion`** (runtime/) — incremental Naive Bayes classifier. Configured via `NaiveBayesConfig` or YAML `GanglionDescriptor.NaiveBayes`.
- **`DroolsGanglion`** (ras-drools/) — Drools CEP stream-mode engine with long-lived/ephemeral sessions, pseudo/realtime clock, hot rule reload.

**Design invariant:** `DetectionResult` must be portable — it may be applied to a different `SituationContext` than the one passed to `detect()` (e.g. after an OCC retry). Implementations must not base decisions on accumulated `context.detections()`.

### CaseInputContributor — domain-specific case seeding

```java
interface CaseInputContributor {
    Map<String, Object> contribute(CaseTriggerConfig config, SituationContext context);
}
```

Discovered via CDI. Output merged into case input data at trigger time. Enables domain-specific case seeding without modifying RAS internals.

### SituationDefinitionProvider — situation registration

```java
interface SituationDefinitionProvider {
    List<SituationRegistration> registrations();
    default List<GanglionDescriptor> ganglionDescriptors() { return List.of(); }
}
```

Register situation definitions programmatically. `YamlSituationDefinitionProvider` in runtime/ is the default classpath-based implementation.

### CorrelationKeyExtractor — custom correlation

```java
@FunctionalInterface
interface CorrelationKeyExtractor {
    String extract(CloudEvent event);
}
```

Default returns `CloudEvent.getSubject()` or `"_singleton"`. Implement for custom correlation logic.

### SituationSource — query active situations

```java
interface SituationSource {
    List<ActiveSituation> activeSituations(String tenancyId);
}
```

Read-only projections of active situations for external consumers.

### SituationQueryService — historical queries

```java
interface SituationQueryService {
    // history() — 3 overloads: tenant, situation, correlation key
    // triggerCount(), trend(), health()
}
```

Query SPI for historical situation data — trends, trigger counts, tenant health.

---

## Core Domain Types

| Type | Purpose |
|------|---------|
| `DetectionResult` | Ganglion output — `ganglionId`, `confidence` (0.0–1.0), `signal`, `evidence` |
| `DetectionSignal` | Signal strength — `NOISE`, `ANTI`, `WEAK`, `DETECTED` (ascending) |
| `SituationContext` | Accumulated detections for a correlation key |
| `SituationDefinition` | Declared situation — event types, correlation window, chain mode, trigger action |
| `ChainMode` | 7 composition strategies: `And`, `Or`, `Threshold`, `Sequence`, `Count`, `Streak`, `Rate` |
| `TriggerAction` | Sealed — `CreateCase(CaseTriggerConfig)` or `NotifyOnly()` |
| `TriggerMode` | Sealed — `FireOnce()` or `Repeating(Duration cooldown)` |
| `TriggerDecision` | 5 outcomes: `TRIGGER`, `TRIGGER_AND_CONTINUE`, `CONTINUE_ACCUMULATING`, `DISCARD`, `RESOLVE` |
| `SituationChangeEvent` | CDI event fired after state transitions — `TRIGGERED`, `RESOLVED`, `DISCARDED` |
| `GanglionDescriptor` | Sealed — `NaiveBayes` or `ExpressionRules` for declarative ganglion configuration |

---

## YAML Situation Definitions

`YamlSituationDefinitionProvider` reads from classpath YAML (default `META-INF/ras-situations.yaml`, configurable via `ras.situations.yaml`). Supports:

- All 7 `ChainMode` variants via `type` discriminator
- `triggerAction`: `type: create-case` or `type: notify-only`
- `triggerMode`: `type: fire-once` (default) or `type: repeating` with `cooldown`
- Optional `correlationKey`, `eventFilter`, `dynamicCaseData` expressions (`jq`, `mvel`)
- YAML ganglia: `type: naive-bayes` or `type: expression-rules`
- Situation templates: `fromTemplate:` + `parameters:` with `${param}` substitution
- Built-in templates: `streak-breach`, `threshold-crossing`, `count-accumulation`, `rate-breach`

---

## CloudEvent Consumption Pattern

RAS is a pure CloudEvent consumer. Platform stream modules produce CloudEvents as CDI async events. RAS observes via `@ObservesAsync CloudEvent`.

**CloudEvent fields used:**
- `type` — routes to matching `SituationDefinition`s and `Ganglion`s
- `time` — temporal ordering (reorder buffer, Drools pseudo clock)
- `subject` — default correlation key (falls back to `"_singleton"`)
- extension `tenancyid` — required; events without it are skipped

**RAS does not produce CloudEvents.** Its output is case creation via `CaseTrigger`.

---

## Dependencies

| Repo | Module | How |
|------|--------|-----|
| `casehub-platform` | `platform-api` | CloudEvent types, CDI event infrastructure |
| `casehub-platform` | `platform` (runtime) | Full platform runtime (runtime module) |
| `casehub-engine` | `engine-api` | `CaseHub` SPI for case creation (via `DefaultCaseTrigger`) |
| Drools 10.1.0 | `drools-model-codegen` | ras-drools module only |

---

## What It Does NOT Do

- **Stream infrastructure** — Kafka, AMQP, webhook, Camel routes live in casehub-platform stream submodules; RAS observes CDI events
- **Case lifecycle management** — only triggers creation; case state machines, milestones, tasks are casehub-engine
- **REST/gRPC endpoints** — entirely event-driven via CDI async events
- **ML model training** — `NaiveBayesGanglion` uses pre-configured priors; no online learning
- **LLM integration (yet)** — `ras-llm` is a POM placeholder with no source
- **Event sourcing or audit trail** — situations are mutable state, deleted when case is created or discarded
- **Distributed coordination** — uses in-process synchronized locks; clustered deployments rely on OCC
- **Rule authoring UI** — DRL provided as classpath resources or programmatic strings
