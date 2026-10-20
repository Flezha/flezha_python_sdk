"""The channel-graph DSL, and the specs it produces.

The graph is built by calling a relay's declaration function with a
:class:`Channels`; every fluent call answers a new :class:`Edge`, so a shared
prefix can be branched without the arms colliding.

Edges are generic in the value that flows on them, which is what makes a wrong
wiring a type error before it is a registration error: ``Edge[Invoice].into``
takes a ``Tasklet[Invoice, O]`` and nothing else, so a type checker rejects
the mismatch in the editor and the registration-time refusal is the backstop
for what a type checker cannot see.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from . import codec as _codec
from . import route_latest as _latest_router
from .call import Response, call_link, response_decoder
from .codec import ChannelCodec
from .errors import FlezhaError
from .tasklet import Tasklet

__all__ = [
    "DROPPED",
    "Channels",
    "ConsumerEdge",
    "Edge",
    "Merge",
    "ChannelSpec",
    "ClassSpec",
    "Link",
    "TaskNode",
    "apply_transforms",
]

#: One link of a chain, exactly as the core deserializes it. A chain is one
#: fluent sequence declared off a channel edge, in the order the author called
#: it; the core reads the consumers, the channel kind, the output and the name off
#: them. Closures never enter the record — they stay on the edge held under the
#: chain that declared them, and ``validate`` attaches them to the consumers the
#: core derived from that same chain.
Link = dict[str, Any]

S = TypeVar("S")
T = TypeVar("T")
U = TypeVar("U")
O = TypeVar("O")


class _Dropped:
    """What a ``filter`` yields for a value it rejects.

    A dropped value is not ``None`` or ``False`` — both are values a handler may
    legitimately carry — so the marker is an object nothing else can produce. It
    takes the value out of the transform chain: an item whose every value was
    dropped is consumed without reaching a handler, and at the prepare crossing
    it answers no key, which is how the core learns not to route it.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return "flezha.DROPPED"

    def __bool__(self) -> bool:
        return False


DROPPED = _Dropped()


class _Fanned:
    """What a ``flat_map`` yields: several values where one arrived.

    A marker rather than the iterable itself, so a ``map`` that legitimately
    answers a list is not mistaken for a fan.
    """

    __slots__ = ("values",)

    def __init__(self, values: Iterable[Any]) -> None:
        self.values = list(values)


def apply_transforms(transforms: Iterable[Callable[[Any], Any]], value: Any) -> list[Any]:
    """Run an edge's transform chain over one value, answering every value it yields.

    One implementation for the three places a chain runs — an entry payload
    before its handler, a payload at the prepare crossing, and an emit on its
    way out — so a filter cannot mean one thing on one path and another
    elsewhere. The answer is a list because that is the shape the three
    transforms share: a dropped value is the empty list, a mapped one a list of
    one and a fan a list of many. A later link runs over every surviving value.
    """
    values = [value]
    for transform in transforms:
        yielded: list[Any] = []
        for current in values:
            answer = transform(current)
            if isinstance(answer, _Fanned):
                yielded.extend(answer.values)
            elif answer is not DROPPED:
                yielded.append(answer)
        values = yielded
    return values


@dataclass(slots=True)
class TaskNode:
    """A tasklet as the graph holds it, with its position in the declaration."""

    index: int
    name: str
    task: Tasklet[Any, Any]
    output: ChannelSpec | None = None
    """The one channel this node produces on, created on first wiring."""


@dataclass(slots=True)
class ConsumerEdge:
    """One consumer of a channel, with the transforms and key of its own edge."""

    tasklet: int
    transforms: list[Callable[[Any], Any]] = field(default_factory=list)
    key_by: Callable[[Any], Any] | None = None


@dataclass(slots=True)
class ChannelSpec:
    """A channel as the graph holds it. Its position in the list IS its id."""

    index: int
    name: str
    kind: str
    producer: int | None = None
    chains: list[list[Link]] = field(default_factory=list)
    """The authoring record: one entry per fluent sequence, in call order."""
    chain_edges: dict[int, ConsumerEdge] = field(default_factory=dict)
    """The edge each chain declared, by chain index.

    Held aside rather than wired: the core answers which chains produced a
    consumer and in what order, so no builder decides which of two
    re-declarations survives.
    """
    consumers: list[ConsumerEdge] = field(default_factory=list)
    """The consumer edges the core derived. Empty until ``validate`` runs."""
    derived_kind: str | None = None
    """The kind the core derived, which ``kind`` (as declared) need not be."""
    overflow: str = "drop_nothing"
    retry_max: int | None = None
    retry_backoff_ms: int | None = None
    keyed: bool = False
    sink: str | None = None
    """The channel's name once a plugin end, a sink or a call, is attached;
    ``None`` means none. The core's answer names no output for a call."""
    decode: Callable[[Any], Any] | None = None
    sink_encode: Callable[[Any], bytes] | None = None
    """The plugin sink's payload encoder, set with the sink by ``output``.

    It belongs to the output frame alone: an inter-tasklet frame is decoded
    with the class codec, so encoding one with this hands the consuming
    tasklet bytes it cannot read.
    """
    call_end: ConsumerEdge | None = None
    """A ``call(...)`` end's chain, applied to each value before it becomes a
    request; its ``tasklet`` is -1, since a call feeds none."""
    codec: str | None = None
    """The format the relay's API announces for an entry's input or an
    output's value: the named codec, ``custom`` for a callable, else JSON."""
    schema: Any = None
    """An entry's declared JSON Schema; ``None`` leaves it to derivation."""
    description: str = ""
    """What an entry channel is for, announced in the relay's API."""


def _branch(tree: Any) -> str | None:
    """The branch a tree setting names: its bare name, or its one key."""
    if tree is False:
        return "false"
    if isinstance(tree, str):
        return tree
    if isinstance(tree, dict) and len(tree) == 1:
        return next(iter(tree))
    return None


@dataclass(slots=True)
class ClassSpec:
    """Everything the graph knows about one declared class."""

    factory_id: str
    tasklets: list[TaskNode]
    channels: list[ChannelSpec]
    description: str | None = None
    max_duration_ms: int | None = None
    run: Any = None
    """The tree's ``run`` as the host declaration writes it; the core judges it."""
    durable: Any = None
    """The tree's ``durable``, likewise; ``None`` is the core's default."""
    compact_after_bytes: int | None = None

    @property
    def runs_in_steps(self) -> bool:
        """Whether this class runs in steps, whose producers release their
        values untransformed.

        A steps stage releases ONE value per emit and the driver fans it out
        to every consumer itself, so each consumer edge — its transforms and
        its key — belongs to the consumer, which applies it on the input the
        core marks untransformed.
        """
        return _branch(self.run) == "steps"

    @property
    def keeps_work(self) -> bool:
        """Whether an instance keeps its work anywhere: ``durable`` is not ``False``."""
        return _branch(self.durable) != "false"


class Channels:
    """The fluent builder a relay's declaration function receives."""

    def __init__(self, factory_id: str) -> None:
        self.factory_id = factory_id
        self.tasklets: list[TaskNode] = []
        self.channels: list[ChannelSpec] = []
        self.compact_after_bytes_value: int | None = None
        self.merge_groups = 0

    # -- class-level declarations ----------------------------------------

    def compact_after_bytes(self, size: int) -> Channels:
        self.compact_after_bytes_value = size
        return self

    # -- the graph --------------------------------------------------------

    def input(
        self,
        name: str,
        *,
        decode: Callable[[Any], Any] | None = None,
        codec: ChannelCodec | None = None,
        description: str = "",
        schema: dict[str, Any] | None = None,
    ) -> Edge[Any]:
        """Declare a controller-fed entry channel.

        ``decode`` turns the wire bytes into the value a handler sees, ``codec``
        names a format from :data:`~flezha.codec.ChannelCodec` instead, and with
        neither the bytes are JSON.

        ``description`` says what the channel is for, in the relay's API. A JSON
        channel's schema is derived from the input type of the tasklet it feeds
        (a ``TypedDict``, a dataclass, or a type with ``model_json_schema()``);
        ``schema`` declares one instead.
        """
        channel = ChannelSpec(index=len(self.channels), name=name, kind="entry")
        named = self.channel_codec(channel, codec, decode)
        channel.decode = decode or _codec.channel_decoder(named)
        channel.codec = "custom" if decode else named or "json"
        channel.description = description
        channel.schema = schema
        self.channels.append(channel)
        return Edge(self, ("entry", channel), {})

    def merge(self) -> Merge:
        """Declare one tasklet fed by several source edges.

        Each ``from_`` names a source and the mapping that lifts its values into
        the merged input, ``key_by`` keys the mapped value on every source, and
        ``into`` closes the merge on its tasklet.
        """
        return Merge(self)

    def route_latest(
        self, documents: Edge[Any], traffic: Edge[T], key: Callable[[T], Any]
    ) -> Edge[tuple[str, Any]]:
        """Hand the latest document to each key on that key's own next traffic.

        Every key ``key`` reads off ``traffic`` receives each distinct document
        once, as a ``(key, document)`` pair; a document whose class-codec bytes
        equal the retained one is inert, and a key with no traffic receives
        nothing. The node is ``route_latest``, so a second router in one
        relay is named with :meth:`route_latest_named`.
        """
        return self._route_latest(documents, traffic, key, None)

    def route_latest_named(
        self, documents: Edge[Any], traffic: Edge[T], key: Callable[[T], Any], name: str
    ) -> Edge[tuple[str, Any]]:
        """:meth:`route_latest` under its own node name."""
        return self._route_latest(documents, traffic, key, name)

    def _route_latest(
        self, documents: Edge[Any], traffic: Edge[T], key: Callable[[T], Any], name: str | None
    ) -> Edge[tuple[str, Any]]:
        # Unnamed passes no name, so the router is an unnamed mention of its
        # tasklet exactly as `into(task)` is.
        return (
            self.merge()
            .from_(documents, _latest_router.document_input)
            .from_(traffic.map(key), _latest_router.traffic_input)
            .into(_latest_router.ROUTER, name)
        )

    def channel_codec(
        self, channel: ChannelSpec, codec: str | None, callable_: object
    ) -> ChannelCodec | None:
        """The codec a channel names, refused when it is unknown or named beside
        the channel's own decoder or encoder; ``None`` when it names none."""
        if codec is None:
            return None
        if callable_ is not None:
            raise FlezhaError("topology.codec_with_callable", {"channel": channel.name})
        if codec not in _codec.CHANNEL_CODECS:
            raise FlezhaError(
                "topology.unknown_codec",
                {"channel": channel.name, "codec": codec, "codecs": list(_codec.CHANNEL_CODECS)},
            )
        return codec

    def node_for(self, task: Tasklet[Any, Any], name: str | None = None) -> TaskNode:
        """The node for a tasklet.

        Two unnamed mentions of one tasklet are one node — the naming
        convention every SDK mirrors — so a diamond that fans out and back in
        has one instance, not two.
        """
        identity = name or task.name
        for node in self.tasklets:
            if node.name == identity and node.task is task:
                return node
        if any(node.name == identity for node in self.tasklets):
            # The node table is keyed by identity and physically cannot hold
            # two, so the record the core would judge is unwritable. The
            # refusal answers under the core's own name all the same.
            raise FlezhaError("topology.node_identity_collision", {"name": identity})
        node = TaskNode(index=len(self.tasklets), name=identity, task=task)
        self.tasklets.append(node)
        return node

    def output_channel_for(self, node: TaskNode) -> ChannelSpec:
        """The channel this node produces on, created on first use.

        Lazily, so a leaf node never carries a phantom channel. Named by the
        node's identity until a ``named(...)`` override, and reused by every
        later call on an edge whose source is this node — which is what makes a
        fan-out one channel with two consumers rather than two channels
        claiming one producer.
        """
        if node.output is not None:
            return node.output
        channel = ChannelSpec(
            index=len(self.channels),
            name=node.name,
            kind="inter_task",
            producer=node.index,
        )
        self.channels.append(channel)
        node.output = channel
        return channel


class Merge:
    """One merge under construction: its sources, and the key they share.

    The builder carries no merge rule of its own. A source named twice or a
    merge of one source reaches the record as written, and the core refuses it.
    """

    __slots__ = ("_builder", "_sources", "_keys")

    def __init__(self, builder: Channels) -> None:
        self._builder = builder
        self._sources: list[Edge[Any]] = []
        self._keys: list[Callable[[Any], Any]] = []

    def from_(self, source: Edge[S], mapping: Callable[[S], Any]) -> Merge:
        """Add a source edge and the mapping that lifts its values into the merged input.

        The mapping composes after any transform already declared on ``source``.
        """
        self._sources.append(source.map(mapping))
        return self

    def key_by(self, key: Callable[[Any], Any]) -> Merge:
        """Key the merged tasklet on the mapped value, from every source."""
        self._keys.append(key)
        return self

    def into(self, task: Tasklet[Any, O], name: str | None = None) -> Edge[O]:
        """Close the merge on a tasklet, and answer that tasklet's output edge."""
        node = self._builder.node_for(task, name)
        group = self._builder.merge_groups
        self._builder.merge_groups += 1
        for source in self._sources:
            # A second key is recorded as a second link, which the core refuses.
            for key in self._keys:
                source = source.key_by(key)
            source._close(node, {"link": "merge", "tasklet": node.index, "group": group})
        return Edge(self._builder, ("tasklet", node), {})


class Edge(Generic[T]):
    """One edge under construction, carrying the type that flows on it.

    Immutable: every fluent call answers a new edge, so a shared prefix can be
    branched without the arms colliding.
    """

    __slots__ = ("_builder", "_from", "_pending")

    def __init__(
        self,
        builder: Channels,
        source: tuple[str, Any],
        pending: dict[str, Any],
    ) -> None:
        self._builder = builder
        self._from = source
        self._pending = pending

    # -- edge options -----------------------------------------------------

    def named(self, name: str) -> Edge[T]:
        """Override the channel name this edge creates.

        The default is derived from the producing node's identity.
        """
        return self._with(channel_name=name)

    def map(self, transform: Callable[[T], U]) -> Edge[U]:
        """Transform every value on this edge. Evaluated producer-side."""
        return self._relink({"link": "transform"}, transforms=[*self._transforms, transform])

    def filter(self, predicate: Callable[[T], bool]) -> Edge[T]:
        """Drop the values a predicate rejects, before this edge's key is computed.

        A dropped value is consumed and reaches no handler.
        """

        def keep(value: Any) -> Any:
            return value if predicate(value) else DROPPED

        return self._relink({"link": "transform"}, transforms=[*self._transforms, keep])

    def flat_map(self, transform: Callable[[T], Iterable[U]]) -> Edge[U]:
        """Turn every value on this edge into the values an iterable yields.

        Each reaches the consumer on its own, and an empty iterable is a drop.
        """

        def fan(value: Any) -> Any:
            return _Fanned(transform(value))

        return self._relink({"link": "transform"}, transforms=[*self._transforms, fan])

    def key_by(self, key: Callable[[T], Any]) -> Edge[T]:
        """Route by a computed key: the consumer becomes a dynamic keyed tasklet."""
        return self._relink({"link": "key"}, key_by=key)

    def on_full(self, policy: str) -> Edge[T]:
        return self._with(overflow=policy)

    def retry_max(self, attempts: int) -> Edge[T]:
        return self._with(retry_max=attempts)

    def retry_backoff_ms(self, milliseconds: int) -> Edge[T]:
        return self._with(retry_backoff_ms=milliseconds)

    def keyed(self) -> Edge[T]:
        """Stamp identity and ``flz.key`` onto emitted plugin frames."""
        return self._with(keyed=True)

    # -- wiring ------------------------------------------------------------

    def branch(self, *arms: Callable[[Edge[T]], Any]) -> Edge[T]:
        """Fan out: each arm receives this same edge.

        ``branch`` builds exactly the topology the separate ``into`` statements
        would. The sugar's arity is recorded, never judged: each arm wires
        itself with ``into(...)`` off this same edge.
        """
        self._channel().chains.append([{"link": "branch", "arms": len(arms)}])
        for arm in arms:
            arm(self)
        return self

    def into(self, task: Tasklet[T, O], name: str | None = None) -> Edge[O]:
        """Wire this edge into a tasklet."""
        node = self._builder.node_for(task, name)
        self._close(node, {"link": "into", "tasklet": node.index})
        return Edge(self._builder, ("tasklet", node), {})

    def output(
        self,
        name: str | None = None,
        *,
        encode: Callable[[Any], bytes] | None = None,
        codec: ChannelCodec | None = None,
    ) -> Edge[T]:
        """Terminate this edge's channel on the plugin layer, and answer the edge.

        The name written here IS the channel's name, everywhere one appears;
        ``output()`` leaves it derived as it would be without an output at all.
        ``output(...).into(...)`` wires a consumer behind the output, so one emit
        reaches both. A channel with no consumer behind it is the plugin-bound
        terminal — terminality is the absence of further wiring, not a
        different method.

        ``encode`` turns a value into the plugin frame's bytes, and ``codec``
        names a format instead. With neither, bytes are sent as they are, a
        string as UTF-8 and any other value as JSON, so an output needs neither for
        a string, bytes or a JSON value.
        """
        link: dict[str, Any] = {"link": "sink"}
        channel = self._plug_channel(name)
        # A chain of its own: the output terminates the channel rather than this
        # edge, so ``into(...)`` still chains after it.
        if name is not None:
            link["name"] = name
            channel.name = name
        named = self._builder.channel_codec(channel, codec, encode)
        channel.chains.append([link])
        if channel.sink is None:
            channel.sink = channel.name
            channel.sink_encode = encode or _codec.channel_encoder(named, channel)
            channel.codec = "custom" if encode else named or "json"
        return self

    def call(
        self,
        target: str | None = None,
        *,
        factory: str | None = None,
        deadline_ms: int,
        create: bool = True,
        channel: str | None = None,
        answer_channel: str | None = None,
        codec: ChannelCodec | None = None,
    ) -> Edge[Response[T, Any]]:
        """End this edge's chain in a call to another relay, and answer the
        edge of its Response channel.

        Each value that reaches the end, after this chain's transforms and key,
        is one request to ``target`` (an instance, or a partition group) or to
        an instance of ``factory`` (after a ``key_by``, the one the key names,
        started unless ``create`` is false). ``channel`` names the callee's entry
        channel, and ``answer_channel`` the callee's channel whose output answers
        (its first output for the request when unset). One ``codec`` writes the request as ``output`` writes and reads
        the answer as ``input`` reads, JSON when none is named. The outcome
        arrives later as a :class:`~flezha.call.Response` on the entry channel
        ``<this channel>.response``, which must feed a tasklet, the calling one
        included. A channel ends in one output or one call.
        """
        spec = self._plug_channel(None)
        named = self._builder.channel_codec(spec, codec, None) or "json"
        response = f"{spec.name}.response"
        # The call closes this edge's chain: its transforms and key are the end's.
        self._record(
            spec,
            call_link(target, factory, deadline_ms, create, channel, answer_channel, response),
        )
        spec.call_end = ConsumerEdge(-1, list(self._transforms), self._pending.get("key_by"))
        if spec.sink is None:
            spec.sink = spec.name
            spec.sink_encode = _codec.channel_encoder(named, spec)
        return self._builder.input(response, decode=response_decoder(_codec.channel_decoder(named)))

    def _plug_channel(self, name: str | None) -> ChannelSpec:
        """The channel a plugin end attaches to; an entry channel has none."""
        if self._from[0] != "tasklet":
            # Not a topology rule the core states: a record whose entry channel
            # carries an output derives a plugin channel with no producer, and
            # there is nothing in it that says which end fed the plugin.
            raise FlezhaError("topology.output_on_entry", {"channel": name or "<derived>"})
        return self._channel()

    # -- internals ---------------------------------------------------------

    @property
    def _transforms(self) -> list[Callable[[Any], Any]]:
        return self._pending.get("transforms", [])

    def _close(self, node: TaskNode, link: Link) -> None:
        """Close this edge's chain onto ``node`` with a terminator link.

        The transforms and the key belong to this edge, not to the channel: one
        channel may feed several tasklets, and a chain declared before one
        `into` must not reach another. The edge is held under the chain that
        declared it and wired by ``validate`` from the core's own answer, so a
        re-declaration is judged there rather than here.
        """
        channel = self._channel()
        recorded = self._record(channel, link)
        channel.chain_edges[recorded["chain"]] = ConsumerEdge(
            tasklet=node.index,
            transforms=list(self._transforms),
            key_by=self._pending.get("key_by"),
        )

    def _with(self, **pending: Any) -> Edge[Any]:
        # Applied now, as the reference applies it: an option no terminator
        # follows would otherwise be dropped.
        edge = Edge(self._builder, self._from, {**self._pending, **pending})
        edge._channel()
        return edge

    def _record(self, channel: ChannelSpec, link: Link) -> dict[str, Any]:
        """Record one link on this edge's chain, forking when another edge has
        already extended it — which is what lets a shared prefix branch without
        the arms colliding. Answers the chain fields the next edge carries."""
        chain = self._pending.get("chain")
        length = self._pending.get("chain_length", 0)
        if chain is None:
            channel.chains.append([link])
            return {"chain": len(channel.chains) - 1, "chain_length": 1}
        links = channel.chains[chain]
        if len(links) == length:
            links.append(link)
            return {"chain": chain, "chain_length": len(links)}
        channel.chains.append([*links[:length], link])
        return {"chain": len(channel.chains) - 1, "chain_length": length + 1}

    def _relink(self, link: Link, **pending: Any) -> Edge[Any]:
        """This edge with one more chain link — the one place a transform's or a
        key's result edge is built, so every link the author declared reaches
        the record whether or not an ``into(...)`` ever closes it."""
        chain = self._record(self._channel(), link)
        return Edge(self._builder, self._from, {**self._pending, **pending, **chain})

    def _channel(self) -> ChannelSpec:
        """The channel this edge's values leave on, with the pending options applied.

        An entry edge already has one; a tasklet edge's is created on first
        use and reused after.
        """
        if self._from[0] == "entry":
            channel: ChannelSpec = self._from[1]
        else:
            channel = self._builder.output_channel_for(self._from[1])
        rename = self._pending.get("channel_name")
        if rename is not None and channel.name != rename:
            # The override is recorded once and applied here too, so the name
            # this host addresses the channel by is the one the core derives
            # from the record — for every terminator, not for all but the last.
            channel.chains.append([{"link": "named", "name": rename}])
            channel.name = rename
        self._apply_pending(channel)
        return channel

    def _apply_pending(self, channel: ChannelSpec) -> None:
        if "overflow" in self._pending:
            channel.overflow = self._pending["overflow"]
        if self._pending.get("retry_max") is not None:
            channel.retry_max = self._pending["retry_max"]
        if self._pending.get("retry_backoff_ms") is not None:
            channel.retry_backoff_ms = self._pending["retry_backoff_ms"]
        if self._pending.get("keyed"):
            channel.keyed = True
