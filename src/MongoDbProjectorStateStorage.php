<?php

namespace Morebec\OrkestraMongoDbAdapter;

use MongoDB\Collection;
use Morebec\Orkestra\EventSourcing\Projecting\ProjectorInterface;
use Morebec\Orkestra\EventSourcing\Projecting\ProjectorStateStorageInterface;

/**
 * Mongo DB Implementation of the ProjectorStateStorage
 */
class MongoDbProjectorStateStorage implements ProjectorStateStorageInterface
{
    public const COLLECTION_NAME = 'projectors.state';

    /**
     * @var Collection
     */
    private $collection;

    public function __construct(MongoDbClient $mongoDbClient)
    {
        $this->collection = $mongoDbClient->getCollection(self::COLLECTION_NAME);
    }

    public function markBroken(ProjectorInterface $projector, string $eventId): void
    {
        $this->collection->updateOne(
            ['_id' => $projector::getTypeName()],
            ['$set' => [
                    'state' => 'BROKEN',
                    'eventId' => $eventId
                ]
            ],
            ['upsert' => true]
        );
    }

    public function markBooting(ProjectorInterface $projector): void
    {
        $this->collection->updateOne(
            ['_id' => $projector::getTypeName()],
            ['$set' => [
                'state' => 'BOOTING',
            ]
            ],
            ['upsert' => true]
        );
    }

    public function markBooted(ProjectorInterface $projector): void
    {
        $this->collection->updateOne(
            ['_id' => $projector::getTypeName()],
            ['$set' => [
                'state' => 'BOOTED',
            ]
            ],
            ['upsert' => true]
        );
    }

    public function markRunning(ProjectorInterface $projector): void
    {
        $this->collection->updateOne(
            ['_id' => $projector::getTypeName()],
            ['$set' => [
                'state' => 'RUNNING',
            ]
            ],
            ['upsert' => true]
        );
    }

    public function markShutdown(ProjectorInterface $projector): void
    {
        $this->collection->updateOne(
            ['_id' => $projector::getTypeName()],
            ['$set' => [
                'state' => 'SHUT DOWN',
            ]
            ],
            ['upsert' => true]
        );
    }
}
