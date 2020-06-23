<?php

namespace Morebec\Orkestra\Adapter\MongoDB;

use InvalidArgumentException;
use MongoDB\Collection;
use Morebec\DomainNormalizer\Mapper\MapperInterface;

abstract class MongoDBAggregateRootRepository
{
    /**
     * @var MapperInterface
     */
    protected $mapper;

    /**
     * @var Collection
     */
    protected $collection;
    /**
     * @var string
     */
    private $aggregateRootClass;

    public function __construct(
        MongoDBClient $client,
        string $collectionName,
        string $aggregateRootClass,
        MapperInterface $mapper)
    {
        if (!$collectionName) {
            throw new InvalidArgumentException('The name of the collection must be set');
        }

        if (!$aggregateRootClass) {
            throw new InvalidArgumentException('The class of the Aggregate Root must be set');
        }

        if (!class_exists($aggregateRootClass)) {
            throw new InvalidArgumentException("The Aggregate Root class $aggregateRootClass does not exist");
        }

        $this->collection = $client->getCollection($collectionName);
        $this->aggregateRootClass = $aggregateRootClass;

        $this->configureMapper($mapper);
    }

    protected function configureMapper(MapperInterface $mapper): void
    {
        $this->mapper = $mapper;
    }

    protected function addAggregateRoot($aggregateRoot): void
    {
        $arr = $this->convertAggregateRootToData($aggregateRoot);
        $this->collection->insertOne($arr);
    }

    protected function findAggregateRootById(string $id)
    {
        return $this->findOneAggregateRootBy(['_id' => $id]);
    }

    protected function findManyAggregateRootsBy(array $filter): array
    {
        $cursor = $this->collection->find($filter, $this->getOptions());
        $cursorResult = $cursor->toArray();

        $self = $this;

        return array_map(static function ($data) use ($self) {
            return $self->convertDataToAggregateRoot($data);
        }, $cursorResult);
    }

    protected function findOneAggregateRootBy(array $filter)
    {
        $data = $this->collection->findOne($filter, $this->getOptions());

        return $this->convertDataToAggregateRoot($data);
    }

    protected function findAllAggregateRoots(): array
    {
        return $this->findManyAggregateRootsBy([]);
    }

    protected function updateAggregateRoot(string $id, $aggregateRoot): void
    {
        $arr = $this->convertAggregateRootToData($aggregateRoot);
        $filter = [
            '_id' => $id,
        ];
        $this->collection->updateOne($filter, ['$set' => $arr]);
    }

    protected function removeAggregateRoot(string $id): void
    {
        $this->collection->deleteOne(['_id' => $id]);
    }

    /**
     * @return string[]
     */
    protected function getOptions(): array
    {
        return [
            // Convert BSON documents to PHP Assoc Array
            'typeMap' => [
                'document' => 'array',
                'root' => 'array',
            ],
        ];
    }

    /**
     * Converts an aggregate root in the form of an array to an instance.
     *
     * @return mixed
     */
    protected function convertDataToAggregateRoot(?array $data)
    {
        if ($data) {
            $data['id'] = $data['_id'];
            unset($data['_id']);
        }

        return $this->mapper->hydrate($this->aggregateRootClass, $data);
    }

    /**
     * Converts an aggregate root to an array.
     */
    private function convertAggregateRootToData($aggregateRoot): array
    {
        $data = $this->mapper->extract($aggregateRoot);
        $data['_id'] = $data['id'];
        unset($data['id']);

        return $data;
    }
}
