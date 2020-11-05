<?php


namespace Morebec\OrkestraMongoDbAdapter\EventStore;

abstract class EventDocument
{
    public const EVENT_ID_KEY = '_id';
    public const STREAM_VERSION_KEY = 'streamVersion';
    public const STREAM_ID_KEY = 'streamId';
    public const METADATA_KEY = 'metadata';
    public const EVENT_PAYLOAD_KEY = 'payload';
    public const EVENT_TYPE_KEY = 'type';

    /** @var string playhead is used to order events in a virtual global stream. */
    public const PLAYHEAD_KEY = 'playhead';

    /** @var string the version of the event. not to be confused with the stream version. */
    const EVENT_VERSION_KEY = 'version';
}
