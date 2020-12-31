<?php

namespace Morebec\OrkestraMongoDbAdapter\EventStore;

use InvalidArgumentException;
use LogicException;
use MongoDB\BSON\UTCDateTime;
use MongoDB\Collection;
use MongoDB\Model\BSONDocument;
use Morebec\Orkestra\DateTime\ClockInterface;
use Morebec\Orkestra\DateTime\DateTime;
use Morebec\Orkestra\EventSourcing\EventStore\CatchupEventStoreSubscriptionInterface;
use Morebec\Orkestra\EventSourcing\EventStore\EventIdInterface;
use Morebec\Orkestra\EventSourcing\EventStore\EventStoreSubscriptionIdInterface;
use Morebec\Orkestra\EventSourcing\EventStore\EventStoreSubscriptionInterface;
use Morebec\Orkestra\EventSourcing\EventStore\EventStreamIdInterface;
use Morebec\Orkestra\EventSourcing\EventStore\EventStreamInterface;
use Morebec\Orkestra\EventSourcing\EventStore\EventTypeInterface;
use Morebec\Orkestra\EventSourcing\EventStore\RecordedEventDescriptorInterface;
use Morebec\Orkestra\EventSourcing\EventStore\StreamedEventCollectionInterface;
use Morebec\Orkestra\EventSourcing\EventStore\StreamNotFoundException;
use Morebec\Orkestra\EventSourcing\SimpleEventStore\CatchupEventStoreSubscription;
use Morebec\Orkestra\EventSourcing\SimpleEventStore\DomainEventDescriptor;
use Morebec\Orkestra\EventSourcing\SimpleEventStore\EventId;
use Morebec\Orkestra\EventSourcing\SimpleEventStore\EventMetadata;
use Morebec\Orkestra\EventSourcing\SimpleEventStore\EventStream;
use Morebec\Orkestra\EventSourcing\SimpleEventStore\EventStreamId;
use Morebec\Orkestra\EventSourcing\SimpleEventStore\EventStreamVersion;
use Morebec\Orkestra\EventSourcing\SimpleEventStore\EventType;
use Morebec\Orkestra\EventSourcing\SimpleEventStore\RecordedEventDescriptor;
use Morebec\Orkestra\EventSourcing\SimpleEventStore\SimpleEventStorageReaderInterface;
use Morebec\Orkestra\EventSourcing\SimpleEventStore\SimpleEventStorageWriterInterface;
use Morebec\Orkestra\EventSourcing\SimpleEventStore\SimpleEventStore;
use Morebec\Orkestra\EventSourcing\SimpleEventStore\StreamedEventCollection;
use Morebec\Orkestra\EventSourcing\Upcasting\UpcastableMessage;
use Morebec\Orkestra\EventSourcing\Upcasting\UpcasterChain;
use Morebec\Orkestra\Messaging\Event\DomainEventInterface;
use Morebec\Orkestra\Messaging\Normalization\DomainMessageNormalizerInterface;
use Morebec\Orkestra\Messaging\VersionedDomainMessageInterface;
use Morebec\OrkestraMongoDbAdapter\MongoDbClient;
use RuntimeException;

/**
 * MongoDB Implementation of SimpleEventStorageReaderInterface and SimpleEventStorageReaderInterface
 * To be used with the Simple Event Store implementation.
 *
 * This implementation relies on the {@link DomainMessageNormalizerInterface} and to the {@link DomainMessageUpcasterProviderInterface}
 * in order to persist restore domain message instances as well as ensuring this process is possible using upcasting.
 *
 * This implementation is still in progress and may be altered in future versions.
 * Requires a MongoDB replica set in order to rely on transactions.
 */
class MongoDbSimpleEventStoreStorage implements SimpleEventStorageReaderInterface, SimpleEventStorageWriterInterface
{
    /** @var string */
    public const EVENTS_COLLECTION_NAME = 'event_store.events';

    /** @var string represents a different collection where all events are stored. */
    public const GLOBAL_STREAM_COLLECTION_NAME = 'event_store.global_events';

    /** @var string */
    public const SUBSCRIPTIONS_COLLECTION_NAME = 'event_store.subscriptions';

    private const SORT_ASCENDING = 1;

    private const SORT_DESCENDING = -1;

    /**
     * @var MongoDbClient
     */
    private $mongoDBClient;

    /**
     * @var Collection
     */
    private $eventsCollection;

    /**
     * @var Collection
     */
    private $subscriptionsCollection;

    /**
     * @var DomainMessageNormalizerInterface
     */
    private $domainMessageNormalizer;
    /**
     * @var UpcasterChain
     */
    private $upcasterChain;
    /**
     * @var ClockInterface
     */
    private $clock;

    public function __construct(
        ClockInterface $clock,
        MongoDbClient $client,
        DomainMessageNormalizerInterface $domainMessageNormalizer,
        UpcasterChain $upcasterChain
    ) {
        $this->mongoDBClient = $client;
        $this->eventsCollection = $this->mongoDBClient->getCollection(self::EVENTS_COLLECTION_NAME)->withOptions([
            'typeMap' => [
                'root' => 'array',
                'document' => 'array'
            ]
        ]);
        $this->eventsCollection->createIndex([EventDocument::STREAM_VERSION_KEY => 1]);
        $this->eventsCollection->createIndex([EventDocument::STREAM_ID_KEY => 1]);

        $this->subscriptionsCollection = $this->mongoDBClient->getCollection(self::SUBSCRIPTIONS_COLLECTION_NAME)->withOptions([
            'typeMap' => [
                'root' => 'array',
                'document' => 'array'
            ]
        ]);
        $this->domainMessageNormalizer = $domainMessageNormalizer;
        $this->upcasterChain = $upcasterChain;
        $this->clock = $clock;
    }

    public function readStreamForward(EventStreamIdInterface $streamId, ?EventIdInterface $eventId = null, int $limit = 0): StreamedEventCollectionInterface
    {
        return $this->readStream(self::SORT_ASCENDING, $streamId, $eventId, $limit);
    }

    public function readStreamBackward(EventStreamIdInterface $streamId, ?EventIdInterface $eventId = null, int $limit = 0): StreamedEventCollectionInterface
    {
        return $this->readStream(self::SORT_DESCENDING, $streamId, $eventId, $limit);
    }

    private function readStream(int $direction, EventStreamIdInterface $streamId, ?EventIdInterface $eventId, int $limit): StreamedEventCollectionInterface
    {
        // First ensure stream exists
        $isGlobalStream = $streamId->isEqualTo(SimpleEventStore::getGlobalStreamId());
        if (!$this->getStream($streamId) && !$isGlobalStream) {
            throw new StreamNotFoundException($streamId);
        }

        // We can either read from the global stream (virtual) or an actual stream.
        // We can either read from a specific event or none.
        $filter = [];
        if ($eventId) {
            $event = $this->eventsCollection->findOne([EventDocument::EVENT_ID_KEY => (string)$eventId]);
            if ($event) {
                $filter[EventDocument::PLAYHEAD_KEY] = [
                    ($direction === self::SORT_ASCENDING) ? '$gt' : '$lt'  => $event[EventDocument::PLAYHEAD_KEY]
                ];
            }
        }

        if (!$isGlobalStream) {
            $filter[EventDocument::STREAM_ID_KEY] = (string)$streamId;
        }

        $options = [
            // Sort in order.
            // (although if using WiredTiger Storage this should be sorted by insertion order which is probably what we want.
            //by default, but we will still use a playhead to ensure this is correctly ordered if cases where the storage is not WiredTiger.)
            'sort' => [EventDocument::PLAYHEAD_KEY => $direction],

            'limit' => $limit,
        ];

        $data = $this->eventsCollection->find($filter, $options);

        $events = [];
        foreach ($data as $datum) {
            $upcastedEvents = $this->upcastNormalizedEvent($datum);
            /** @var UpcastableMessage $event */
            foreach ($upcastedEvents as $event) {
                $datum[EventDocument::EVENT_PAYLOAD_KEY] = $event->data;
                $events[] = $this->denormalizeRecordedEventDescriptor($datum);
            }
        }

        return new StreamedEventCollection($streamId, $events);
    }

    public function getStream(EventStreamIdInterface $streamId): ?EventStreamInterface
    {
        if ($streamId === SimpleEventStore::getGlobalStreamId()) {
            throw new LogicException('Cannot get the global stream as it is a virtual stream.');
        }

        $filter[EventDocument::STREAM_ID_KEY] = (string)$streamId;

        $filter = [
            EventDocument::STREAM_ID_KEY => (string) $streamId,
        ];

        $options = [
            // Sort by descending so the first event found would be the one with the highest version number which
            // Would correspond to the stream's version.
            'sort' => [EventDocument::EVENT_VERSION_KEY => self::SORT_DESCENDING],
        ];

        $data = $this->eventsCollection->findOne($filter, $options);

        if (!$data) {
            return null;
        }

        return new EventStream(
            EventStreamId::fromString($streamId),
            EventStreamVersion::fromInt($data[EventDocument::STREAM_VERSION_KEY])
        );
    }

    public function createStream(EventStream $stream): void
    {
        // There's actually nothing to be done here, since streams are not documents. Events are.
    }

    public function appendToStream(EventStreamIdInterface $streamId, iterable $recordedEvents): void
    {
        $documents = [];
        /** @var RecordedEventDescriptor $eventDescriptor */
        foreach ($recordedEvents as $eventDescriptor) {
            $event = $eventDescriptor->getEvent();
            $eventIsVersioned = $event instanceof VersionedDomainMessageInterface;

            $eventData = $this->normalizeEvent($event);

            $documents[] = [
                EventDocument::EVENT_ID_KEY => (string) $eventDescriptor->getEventId(),
                EventDocument::STREAM_ID_KEY => (string) $streamId,
                EventDocument::STREAM_VERSION_KEY => $eventDescriptor->getStreamVersion()->toInt(),
                EventDocument::METADATA_KEY => $eventDescriptor->getEventMetadata()->toArray(),
                EventDocument::EVENT_TYPE_KEY => (string) $eventDescriptor->getEventType(),
                EventDocument::EVENT_PAYLOAD_KEY => $eventData,
                EventDocument::PLAYHEAD_KEY => $this->clock->now()->getMillisTimestamp(),
                EventDocument::EVENT_VERSION_KEY => $eventIsVersioned ? $event::getMessageVersion() : 0,
                EventDocument::EVENT_RECORDED_AT_KEY => new UTCDateTime($this->clock->now()->getTimestamp() * 1000)
            ];
        }

        if (!$documents) {
            return;
        }

        $this->eventsCollection->insertMany($documents, ['session' => $this->mongoDBClient->getSession()]);
    }

    public function getSubscription(EventStoreSubscriptionIdInterface $subscriptionId): ?EventStoreSubscriptionInterface
    {
        /** @var BSONDocument $data */
        $data = $this->subscriptionsCollection->findOne(
            [SubscriptionDocument::ID_KEY => (string) $subscriptionId]
        );

        if (!$data) {
            return null;
        }

        $lastReadEventId = $data[SubscriptionDocument::LAST_READ_EVENT_ID_KEY];

        return new CatchupEventStoreSubscription(
            $subscriptionId,
            EventStreamId::fromString($data[EventDocument::STREAM_ID_KEY]),
            array_map(static function (string $type) {
                return EventType::fromString($type);
            }, $data[SubscriptionDocument::EVENT_TYPES_KEY]),
            $lastReadEventId ? EventId::fromString($lastReadEventId) : null
        );
    }

    public function startSubscription(EventStoreSubscriptionInterface $subscription): void
    {
        $document = [
            SubscriptionDocument::ID_KEY => (string) $subscription->getId(),
            EventDocument::STREAM_ID_KEY => (string) $subscription->getStreamId(),
            SubscriptionDocument::EVENT_TYPES_KEY => array_map(static function (EventTypeInterface $eventType) {
                return (string) $eventType;
            }, $subscription->getTypeFilter()),
        ];

        if ($subscription instanceof CatchupEventStoreSubscriptionInterface) {
            $document[SubscriptionDocument::LAST_READ_EVENT_ID_KEY] = $subscription->getLastEventId();
        }

        $this->subscriptionsCollection->insertOne(
            $document,
            ['session' => $this->mongoDBClient->getSession()]
        );
    }

    public function cancelSubscription(EventStoreSubscriptionIdInterface $subscriptionId): void
    {
        $this->subscriptionsCollection->deleteOne(
            [SubscriptionDocument::ID_KEY => (string) $subscriptionId],
            ['session' => $this->mongoDBClient->getSession()]
        );
    }

    public function resetSubscription(EventStoreSubscriptionIdInterface $subscriptionId): void
    {
        $this->subscriptionsCollection->updateOne(
            ['_id' => (string) $subscriptionId],
            ['$set' => [SubscriptionDocument::LAST_READ_EVENT_ID_KEY => null]],
            ['session' => $this->mongoDBClient->getSession()]
        );
    }

    public function advanceSubscription(EventStoreSubscriptionIdInterface $subscriptionId, EventIdInterface $eventId): void
    {
        $subscription = $this->getSubscription($subscriptionId);
        if (!$subscription instanceof CatchupEventStoreSubscriptionInterface) {
            throw new RuntimeException('Cannot only advance a CatchupSubscription');
        }

        $this->subscriptionsCollection->updateOne(
            ['_id' => (string) $subscriptionId],
            ['$set' => [SubscriptionDocument::LAST_READ_EVENT_ID_KEY => (string) $eventId]],
            ['session' => $this->mongoDBClient->getSession()]
        );
    }

    /**
     * Upcasts an event and returns the result as an array as when upcasting an event might
     * have been split into many new ones.
     * @param array $data
     * @return array
     */
    private function upcastNormalizedEvent(array $data): array
    {
        $metadata = $data[EventDocument::METADATA_KEY];
        $metadata['eventTypeName'] = $data[EventDocument::EVENT_TYPE_KEY];
        $metadata['playhead'] = $data[EventDocument::PLAYHEAD_KEY];
        return $this->upcasterChain->upcast(new UpcastableMessage($data[EventDocument::EVENT_PAYLOAD_KEY], $metadata));
    }

    /**
     * Denormalizes data read from MongoDB into an event descriptor interface.
     */
    protected function denormalizeRecordedEventDescriptor(array $data): RecordedEventDescriptorInterface
    {
        $metadata = $data[EventDocument::METADATA_KEY];

        $event = $this->domainMessageNormalizer->denormalize($data[EventDocument::EVENT_PAYLOAD_KEY], $data[EventDocument::EVENT_TYPE_KEY]);
        if (!$event instanceof DomainEventInterface) {
            throw new InvalidArgumentException(sprintf("Unexpected Domain Message Type: %s", get_debug_type($event)));
        }

        $descriptor = DomainEventDescriptor::forDomainEvent(
            EventId::fromString($data[EventDocument::EVENT_ID_KEY]),
            $event,
            new EventMetadata($metadata)
        );

        /** @var UTCDateTime $recordedAt */
        $recordedAt = $data[EventDocument::EVENT_RECORDED_AT_KEY];
        $data[EventDocument::EVENT_RECORDED_AT_KEY] = new DateTime($recordedAt->toDateTime());

        return RecordedEventDescriptor::fromEventDescriptor(
            $descriptor,
            EventStreamId::fromString($data[EventDocument::STREAM_ID_KEY]),
            EventStreamVersion::fromInt($data[EventDocument::STREAM_VERSION_KEY]),
            $data[EventDocument::EVENT_RECORDED_AT_KEY]
        );
    }

    /**
     * Normalizes a Domain Event.
     * @param DomainEventInterface $event
     * @return array
     */
    protected function normalizeEvent(DomainEventInterface $event): array
    {
        return $this->domainMessageNormalizer->normalize($event);
    }
}
