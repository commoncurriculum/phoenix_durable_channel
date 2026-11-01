import { Schema } from "effect"

const Ref = Schema.NullOr(Schema.String)

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

export const decodeSyncReply = Schema.decodeUnknownSync(Schema.Struct({ received: Schema.Number }))

export const decodeReadReply = Schema.decodeUnknownSync(
  Schema.Struct({ sent: Schema.Number, epoch: Schema.String }),
)

export const isNotTaken = Schema.is(
  Schema.Union(
    Schema.Struct({ phx_not_taken: Schema.String }),
    Schema.Struct({ reason: Schema.Literal("unmatched topic") }),
  ),
)

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

export const hasPublishedReader = Schema.is(Schema.Struct({ phx_reader: PublishedReader }))

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

export const decodeDurableStreamsControl = Schema.decodeUnknownSync(
  Schema.parseJson(Schema.Struct({ streamNextOffset: Schema.String })),
)
