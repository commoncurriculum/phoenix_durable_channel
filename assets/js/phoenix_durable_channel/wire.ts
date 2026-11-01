import { Schema } from "effect"

// What the server sends, checked as it is read, so nothing downstream takes its shape on trust.

const Ref = Schema.NullOr(Schema.String)

/** `[join_ref, ref, topic, event, payload]`, and a durable message's number after it. */
export const decodeFrame = Schema.decodeUnknownSync(
  Schema.parseJson(
    Schema.Tuple(
      Ref,
      Ref,
      Schema.String,
      Schema.String,
      Schema.Unknown,
      Schema.optionalElement(Schema.Number),
    ),
  ),
)

export const decodeReply = Schema.decodeUnknownSync(
  Schema.Struct({ status: Schema.String, response: Schema.Unknown }),
)

/** The answer to a `phx_sync`: the last durable message the server received from the client. */
export const decodeSyncReply = Schema.decodeUnknownSync(Schema.Struct({ received: Schema.Number }))

/** The answer to a `phx_read`: the last durable message the server sent, and the store's epoch. */
export const decodeReadReply = Schema.decodeUnknownSync(
  Schema.Struct({ sent: Schema.Number, epoch: Schema.String }),
)

/**
 * Whether an error reply says the server did not run a durable message: refused as not
 * taken, or Phoenix's "unmatched topic", for a message that reached the connection after
 * its channel process stopped, such as one replaced by a late join.
 */
export const isNotTaken = Schema.is(
  Schema.Union(
    Schema.Struct({ phx_not_taken: Schema.String }),
    Schema.Struct({ reason: Schema.Literal("unmatched topic") }),
  ),
)

/** A long-poll answer. Only a 200 carries messages, and only a 200 or 410 a token. */
export const decodePollResponse = Schema.decodeUnknownSync(
  Schema.Struct({
    status: Schema.Number,
    token: Schema.optionalWith(Schema.NullOr(Schema.String), {
      default: () => null,
    }),
    messages: Schema.optionalWith(Schema.mutable(Schema.Array(Schema.String)), {
      default: () => [],
    }),
  }),
)

const PresenceMeta = Schema.mutable(
  Schema.Struct(
    { phx_ref: Schema.String },
    Schema.Record({ key: Schema.String, value: Schema.Unknown }),
  ),
)
const PresenceState = Schema.mutable(
  Schema.Record({
    key: Schema.String,
    value: Schema.mutable(
      Schema.Struct(
        { metas: Schema.mutable(Schema.Array(PresenceMeta)) },
        Schema.Record({ key: Schema.String, value: Schema.Unknown }),
      ),
    ),
  }),
)

export const decodePresenceState = Schema.decodeUnknownSync(PresenceState)

export const decodePresenceDiff = Schema.decodeUnknownSync(
  Schema.mutable(Schema.Struct({ joins: PresenceState, leaves: PresenceState })),
)

/** The data of an S2 `batch` event. A record whose first header has an empty name is a command. */
export const decodeS2Batch = Schema.decodeUnknownSync(
  Schema.parseJson(
    Schema.Struct({
      records: Schema.Array(
        Schema.Struct({
          seq_num: Schema.Number,
          headers: Schema.optionalWith(Schema.Array(Schema.Tuple(Schema.String, Schema.String)), {
            default: () => [],
          }),
          body: Schema.String,
        }),
      ),
    }),
  ),
)

/** The body the server writes for each durable message on an S2 stream. */
export const decodeStreamBody = Schema.decodeUnknownSync(
  Schema.parseJson(Schema.Struct({ event: Schema.String, payload: Schema.Unknown })),
)

const PublishedReader = Schema.Struct(
  {
    type: Schema.String,
    last: Schema.Number,
    epoch: Schema.optionalWith(Schema.String, { exact: true }),
  },
  Schema.Record({ key: Schema.String, value: Schema.Unknown }),
)

/** Where a join reply says to read the channel's durable messages, if it says. */
export const hasPublishedReader = Schema.is(Schema.Struct({ phx_reader: PublishedReader }))

/** The answer to `phx_reader`: where to read, with a fresh token. */
export const decodePublishedReader = Schema.decodeUnknownSync(PublishedReader)
export const decodeS2Reader = Schema.decodeUnknownSync(
  Schema.Struct({
    endpoint: Schema.String,
    basin: Schema.String,
    stream: Schema.String,
    token: Schema.NullOr(Schema.String),
  }),
)

export const decodeDurableStreamsReader = Schema.decodeUnknownSync(
  Schema.Struct({
    url: Schema.String,
    token: Schema.NullOr(Schema.String),
    last: Schema.Number,
    offset: Schema.String,
  }),
)

/** The data of a Durable Streams `data` event on a JSON stream: the messages, in order. */
export const decodeDurableStreamsData = Schema.decodeUnknownSync(
  Schema.parseJson(
    Schema.Array(
      Schema.Struct({
        number: Schema.Number,
        event: Schema.String,
        payload: Schema.Unknown,
      }),
    ),
  ),
)

/** The data of a Durable Streams `control` event: where the next read starts. */
export const decodeDurableStreamsControl = Schema.decodeUnknownSync(
  Schema.parseJson(Schema.Struct({ streamNextOffset: Schema.String })),
)
