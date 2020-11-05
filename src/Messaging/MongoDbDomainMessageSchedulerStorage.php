<?php

namespace Morebec\OrkestraMongoDbAdapter\Messaging;

use MongoDB\BSON\UTCDateTime;
use MongoDB\Collection;
use Morebec\Orkestra\DateTime\DateTime;
use Morebec\Orkestra\Messaging\Command\DomainCommandInterface;
use Morebec\Orkestra\Messaging\DomainMessageHeaders;
use Morebec\Orkestra\Messaging\DomainMessageInterface;
use Morebec\Orkestra\Messaging\Event\DomainEventInterface;
use Morebec\Orkestra\Messaging\Normalization\DomainMessageNormalizerInterface;
use Morebec\Orkestra\Messaging\Query\DomainQueryInterface;
use Morebec\Orkestra\Messaging\Scheduling\DomainMessageSchedulerStorageInterface;
use Morebec\Orkestra\Messaging\Scheduling\ScheduledDomainMessageWrapper;
use Morebec\Orkestra\Normalization\ObjectNormalizerInterface;
use Morebec\OrkestraMongoDbAdapter\MongoDbClient;

class MongoDbDomainMessageSchedulerStorage implements DomainMessageSchedulerStorageInterface
{
    /** @var string */
    public const COLLECTION_NAME = 'domain.scheduled_messages';

    /**
     * @var DomainMessageNormalizerInterface
     */
    private $domainMessageNormalizer;

    /**
     * @var Collection
     */
    private $collection;
    /**
     * @var ObjectNormalizerInterface
     */
    private $objectNormalizer;

    public function __construct(
        MongoDbClient $mongoDBClient,
        DomainMessageNormalizerInterface $domainMessageNormalizer,
        ObjectNormalizerInterface $objectNormalizer
    ) {
        $this->domainMessageNormalizer = $domainMessageNormalizer;
        $this->collection = $mongoDBClient->getCollection(self::COLLECTION_NAME);
        $this->objectNormalizer = $objectNormalizer;
    }

    public function add(ScheduledDomainMessageWrapper $messageWrapper): void
    {
        $message = $messageWrapper->getMessage();
        $messageHeaders = $messageWrapper->getMessageHeaders();
        $data = [
            '_id' => $messageWrapper->getMessageId(),
            'scheduledAt' => $messageHeaders->get(DomainMessageHeaders::SCHEDULED_AT),
            'message' => $this->domainMessageNormalizer->normalize($message),
            'messageHeaders' => $messageHeaders->toArray(),
            'messageTypeName' => $message::getTypeName(),
            'messageType' => $this->getMessageType($message)
        ];

        $this->collection->insertOne($data);
    }

    public function findByDateTime(DateTime $from, DateTime $to): array
    {
        $results = $this->collection->find(
            [
                'scheduledAt' => [
                    '$gte' => $from->getMillisTimestamp(),
                    '$lte' => $to->getMillisTimestamp()
                ]
            ],
            ['typeMap' => ['document' => 'array', 'root' => 'array']]
        );

        return $this->denormalizeScheduledMessageWrappers($results);
    }

    public function findScheduledBefore(DateTime $dateTime): array
    {
        $results = $this->collection->find(
            [
                'scheduledAt' => [
                    '$lte' => $dateTime->getMillisTimestamp()
                ]
            ],
            ['typeMap' => ['document' => 'array', 'root' => 'array']]
        );

        return $this->denormalizeScheduledMessageWrappers($results);
    }

    public function remove(ScheduledDomainMessageWrapper $messageWrapper): void
    {
        $messageHeaders = $messageWrapper->getMessageHeaders();
        $this->collection->deleteOne(['_id' => (string)$messageHeaders->get(DomainMessageHeaders::MESSAGE_ID)]);
    }

    /**
     * @param iterable $results
     * @return array
     */
    private function denormalizeScheduledMessageWrappers(iterable $results): iterable
    {
        $wrappers = [];
        foreach ($results as $data) {
            $message = $this->domainMessageNormalizer->denormalize($data['message']);
            $messageHeaders = new DomainMessageHeaders($data['messageHeaders']);
            $wrappers[] = ScheduledDomainMessageWrapper::wrap($message, $messageHeaders);
        }

        return $wrappers;
    }

    /**
     * Returns the type of a message (command, event, query)
     * @param DomainMessageInterface $domainMessage
     * @return string
     */
    private function getMessageType(DomainMessageInterface $domainMessage): string
    {
        if ($domainMessage instanceof DomainCommandInterface) {
            return 'command';
        }

        if ($domainMessage instanceof DomainEventInterface) {
            return 'event';
        }

        if ($domainMessage instanceof DomainQueryInterface) {
            return 'query';
        }

        return 'generic';
    }
}
