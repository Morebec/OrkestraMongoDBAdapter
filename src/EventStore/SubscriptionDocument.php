<?php


namespace Morebec\OrkestraMongoDbAdapter\EventStore;

abstract class SubscriptionDocument
{
    /** @var string  */
    public const ID_KEY = '_id';

    /** @var string  */
    public const EVENT_TYPES_KEY = 'eventTypes';

    // Catchup Subscriptions.
    /** @var string  */
    const LAST_READ_EVENT_ID_KEY = 'lastReadEventId';
}
