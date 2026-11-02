# Tenets for moving OpenSearch's inter-node protocol to protobuf

These are the commitments the migration is held to. When a design choice is in tension with one of them, the
tenet wins or the choice is written down as an explicit exception.

## 1. Developer experience after cutover

The point of the migration is what it is like to add or change an inter-node message once the legacy encoding
is gone. Every tenet here describes that end state; the compatibility window is scaffolding and must not shape
the end state.

**The schema is the source of truth for serialization.** A message's wire fields are declared once, in a
`.proto` file next to the Java code that uses it. No class hand-writes serialization: no `writeTo`, no
`StreamInput` constructor, no `Writeable.Reader`. Adding a field is one line in the schema, an accessor on the
request or response class, and the logic that reads it.

**Compatibility is checked by a tool, not by a reviewer.** `buf breaking` against the previous release tag runs
in `precommit`. Removing a field, changing a type, or reusing a field number fails the build. Nobody has to
remember a rule.

**No version guards in message code.** Additive changes need none: an old peer ignores fields it doesn't know,
a new peer sees "absent" for fields an old peer didn't send. The only version checks left are semantic ones
("the peer interprets this value differently"), and those live in the action's logic where the semantics are,
not in serialization.

**Failures are local and legible.** A malformed payload fails at the boundary of the message that is malformed,
with a parse error naming it. It does not desynchronise the stream and surface as a nonsense value three fields
later in an unrelated class.

**The generated message is the only state; the class's accessors are the only way to read it.** A request or
response class holds its generated message as its only field. Code outside the class reads it through the
class's accessors, which own defaults, sentinels and conversion to domain types (`TimeValue`, `Priority`,
`ActiveShardCount`, ...). Transport actions are written against the request and response classes, not the
generated types, and the generated types are not exposed outside the class's package. `equals`, `hashCode` and
`toString` come from the message.

The schema is shaped by the wire (every scalar `optional`, `_UNSPECIFIED` enums, no defaults); logic needs
defaults applied and types with behaviour. Accessors are where one becomes the other, written once per field.
If logic read the message directly, every read would have to repeat the presence check and the default, and a
missed one (`getTimeout()` on an absent timeout) fails at runtime, not at compile time. The public Java API
already requires these accessors to exist indefinitely, so reading through them adds no duplication.

**Absent is distinguishable from default.** Every scalar is `optional`. Enums carry an explicit
`_UNSPECIFIED = 0`. Sentinels (`-1` meaning "all", `-2` meaning "default") are modelled as what they mean, not
as magic numbers a reader has to know.

**Generated code is a build artifact, not a maintenance burden.** It is excluded from license-header,
public-API-annotation and formatting checks. Nobody edits it, nobody reviews it, nobody annotates it.

**Mechanical work is mechanical.** Rewriting a request/response class as an envelope, porting a `writeTo` into a
frozen `writeLegacy`, generating golden bytes from the previous release's artifact: each is a repeatable recipe
that an agent can execute and a differential test can verify. The human reviews the schema and the accessors,
not the serialization. The transport action does not change.

**The split lives in one place.** Header fields versus payload, and protobuf versus legacy payload, are decided
at the root of the message hierarchy (`TransportRequest`, `TransportResponse`), not in a parallel set of base
classes. Opting a message in is implementing one interface. When the compatibility window closes, the legacy
path is deleted as a unit: one branch at the root and every `writeLegacy`/`readLegacy`. Nothing is left to clean
up per request family or per field.

## 2. Transparent to end users

A user, operator or plugin author must not be able to tell that the encoding changed, except by reading the
release notes. Concretely:

**Action names do not change.** Security roles grant `cluster:monitor/health`, exactly or by wildcard; the task
management API and the security audit log report it. A migrated action keeps its name. Encoding is a property of
the message class, selected per peer version, never a property of the action. There is no `.../proto` variant.

**REST output is byte-identical.** Field order, duplicated legacy fields (`discovered_master`), `human`
formatting, `level` behaviour, HTTP status codes. Differential tests compare rendered JSON against the previous
implementation and against output captured from the previous release.

**Error behaviour is identical.** Same exception types, same validation messages, same status codes for
timeouts and missing indices.

**Rolling upgrades need no operator action.** A mixed-version cluster serves every API from every node.
New→new uses protobuf; new→old and old→new use the frozen legacy layout, chosen by the peer's version exactly
as every previous BWC change in OpenSearch has been. The legacy layout is pinned to bytes produced by the
released previous version, not to a copy of the old code.

**The Java API does not change.** Request and response classes keep their names, their place in the
`ActionRequest`/`ActionResponse` hierarchy, and every public method. `client.admin().cluster().health(...)`,
request builders and `ActionType` singletons work unchanged. `japicmp` against the previous release is the gate.

**Plugin extension points keep working.** `ActionFilter` chains, `TransportInterceptor`, `instanceof
IndicesRequest.Replaceable` (how the security plugin rewrites index names), task parent IDs and `ThreadContext`
header propagation all see the same objects they saw before.

**The transport layer is untouched.** Protobuf payloads ride inside the existing OpenSearch transport protocol.
Ports, TLS, inter-node authentication, connection profiles, compression and circuit breakers are not involved
and not changed.

**Nothing is removed before its time.** The legacy encoding stays for the full compatibility window the project
already guarantees. It is deleted at a major version boundary, together, with a release note, and not before.

**Visible behaviour changes are listed, not discovered.** Where strict preservation is impossible or wrong
(a `null` that became an empty array because the `null` was a latent NPE; an `equals` that now compares fields
the old wire silently dropped), the change is small, deliberate, and written in the PR and the findings
document. "Transparent" means no surprises, not that nothing is ever allowed to improve.

