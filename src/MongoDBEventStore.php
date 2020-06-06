<?php

namespace Morebec\Orkestra\Adapter\MongoDB;

use MongoDB\Collection;
use MongoDB\Model\BSONDocument;
use Morebec\DomainNormalizer\Denormalization\Configuration\DenormalizerConfiguration;
use Morebec\DomainNormalizer\Denormalization\Denormalizer;
use Morebec\DomainNormalizer\Normalization\Configuration\NormalizerConfiguration;
use Morebec\DomainNormalizer\Normalization\Normalizer;
use Morebec\Orkestra\Adapter\MongoDB\Normalization\EventDescriptorNormalizationFactory;
use Morebec\Orkestra\Adapter\MongoDB\Normalization\EventNormalizationFactory;
use Morebec\Orkestra\EventSourcing\EventStore\EventDescriptor;
use Morebec\Orkestra\EventSourcing\EventStore\EventStoreInterface;
use Morebec\Orkestra\EventSourcing\EventStore\EventStreamVersionMismatchException;

class MongoDBEventStore implements EventStoreInterface
{
    public const SORT_ASCENDING = 1;

    public const SORT_DESCENDING = -1;

    /** @var Collection */
    private $eventsCollection;

    /** @var Normalizer */
    private $normalizer;

    /** @var Denormalizer */
    private $denormalizer;

    public function __construct(MongoDBClient $client)
    {
        $this->eventsCollection = $client->getCollection('event_store');

        // TODO Add Indexes for version and stream fields

        // Normalization
        $normalizerConfiguration = new NormalizerConfiguration();
        $normalizerConfiguration->registerDefinition(EventNormalizationFactory::getNormalizationDefinition());
        $normalizerConfiguration->registerDefinition(EventDescriptorNormalizationFactory::getNormalizationDefinition());
        $this->normalizer = new Normalizer($normalizerConfiguration);

        $denormalizerConfiguration = new DenormalizerConfiguration();
        $denormalizerConfiguration->registerDefinition(EventNormalizationFactory::getDenormalizationDefinition());
        $denormalizerConfiguration->registerDefinition(EventDescriptorNormalizationFactory::getDenormalizationDefinition());
        $this->denormalizer = new Denormalizer($denormalizerConfiguration);
    }

    /**
     * {@inheritdoc}
     */
    public function appendToStream(string $streamName, int $expectedVersion, iterable $events): void
    {
        $streamVersion = $this->findEventStreamVersion($streamName);

        // Concurrency check
        if ($streamVersion !== $expectedVersion) {
            throw new EventStreamVersionMismatchException($streamName, $expectedVersion, $streamVersion);
        }

        $eventVersion = $expectedVersion;
        /** @var EventDescriptor $event */
        foreach ($events as $event) {
            $eventVersion++;

            $data = $this->normalizer->normalize($event);

            $data['stream'] = $streamName;
            $data['version'] = $eventVersion;

            $this->eventsCollection->insertOne($data);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function readStreamAtVersion(string $streamName, int $version): ?EventDescriptor
    {
        /** @var BSONDocument|null $data */
        $data = $this->eventsCollection->findOne([
            'stream' => $streamName,
            'version' => $version,
        ]);

        if (!$data) {
            return null;
        }

        /** @var EventDescriptor $descriptor */
        $descriptor = $this->denormalizer->denormalize($data->getArrayCopy(), EventDescriptor::class);

        return $descriptor;
    }

    /**
     * {@inheritdoc}
     */
    public function readStreamAtVersionForward(string $streamName, int $startVersion, bool $includeStart = true): iterable
    {
        $comparator = $includeStart ? '$gt' : '$gte';

        $filter = [
            'stream' => $streamName,
            'version' => [
                $comparator => $startVersion,
            ],
        ];

        $options = [
            'sort' => ['occurredAt' => self::SORT_ASCENDING],

            // Convert BSON documents to PHP Assoc Array
            'typeMap' => [
                'document' => 'array',
                'root' => 'array',
            ],
        ];

        $data = $this->eventsCollection->find($filter, $options);

        foreach ($data as $d) {
            yield $this->denormalizer->denormalize($d, EventDescriptor::class);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function readStreamFromStartForward(string $streamName): iterable
    {
        $filter = [
            'stream' => $streamName,
        ];

        $options = [
            'sort' => ['occurredAt' => self::SORT_ASCENDING],

            // Convert BSON documents to PHP Assoc Array
            'typeMap' => [
                'document' => 'array',
                'root' => 'array',
            ],
        ];

        $data = $this->eventsCollection->find($filter, $options);

        foreach ($data as $d) {
            yield $this->denormalizer->denormalize($d, EventDescriptor::class);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function readAllFromTimestampForward(float $timestamp): iterable
    {
        $filter = [
            'version' => [
                '$gte' => $timestamp,
            ],
        ];

        $options = [
            'sort' => ['occurredAt' => self::SORT_ASCENDING],

            // Convert BSON documents to PHP Assoc Array
            'typeMap' => [
                'document' => 'array',
                'root' => 'array',
            ],
        ];

        $data = $this->eventsCollection->find($filter, $options);

        foreach ($data as $d) {
            yield $this->denormalizer->denormalize($d, EventDescriptor::class);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function readAllFromEventIdForward(string $eventId, bool $includeStart = true): iterable
    {
        // Find the timestamp of the event
        $evtData = $this->eventsCollection->findOne(['_id' => $eventId]);
        if (!$evtData) {
            return [];
        }

        // Replay from that timestamp
        yield from $this->readAllFromTimestampForward($evtData['occurredAt']);
    }

    /**
     * {@inheritdoc}
     */
    public function readLatest(): ?EventDescriptor
    {
        $filter = [];

        $options = [
            'sort' => ['occurredAt' => self::SORT_DESCENDING],

            // Convert BSON documents to PHP Assoc Array
            'typeMap' => [
                'document' => 'array',
                'root' => 'array',
            ],
        ];

        $data = $this->eventsCollection->findOne($filter, $options);

        if (!$data) {
            return null;
        }

        /** @var EventDescriptor $latest */
        $latest = $this->denormalizer->denormalize($data, EventDescriptor::class);

        return $latest;
    }

    /**
     * {@inheritdoc}
     */
    public function findEventStreamVersion(string $streamName): int
    {
        $filter = [
            'stream' => $streamName,
        ];

        $options = [
            'sort' => ['version' => self::SORT_DESCENDING],

            // Convert BSON documents to PHP Assoc Array
            'typeMap' => [
                'document' => 'array',
                'root' => 'array',
            ],
        ];

        $data = $this->eventsCollection->findOne($filter, $options);

        if (!$data) {
            return -1;
        }

        return $data['version'];
    }
}
