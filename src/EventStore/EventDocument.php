<?php


namespace Morebec\OrkestraMongoDbAdapter\EventStore;

abstract class EventDocument
{
    /** @var string id of the event. */
    public const EVENT_ID_KEY = '_id';

    /** @var string version of the stream associated with this event. */
    public const STREAM_VERSION_KEY = 'streamVersion';

    /** @var string the id of the stream into which this event was added. */
    public const STREAM_ID_KEY = 'streamId';

    /** @var string metadata about the event. */
    public const METADATA_KEY = 'metadata';

    /** @var string payload of the event. */
    public const EVENT_PAYLOAD_KEY = 'payload';

    /** @var string Type of the event */
    public const EVENT_TYPE_KEY = 'type';

    /** @var string playhead is used to order events in a virtual global stream. */
    public const PLAYHEAD_KEY = 'playhead';

    /** @var string the version of the event's payload. not to be confused with the stream version. */
    const EVENT_VERSION_KEY = 'payloadVersion';

    /** @var string date time at which this event was recorded. */
    const EVENT_RECORDED_AT_KEY = 'recordedAt';
}
