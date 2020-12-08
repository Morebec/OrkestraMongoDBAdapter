<?php

namespace Morebec\OrkestraMongoDbAdapter;

use InvalidArgumentException;
use MongoDB\Collection;
use Morebec\Orkestra\Normalization\ObjectNormalizerInterface;

/**
 * Simple Abstract implementation of a State based Aggregate Root Repository.
 */
abstract class AbstractMongoDBAggregateRootRepository
{
    /**
     * @var ObjectNormalizerInterface
     */
    protected $normalizer;

    /**
     * @var Collection
     */
    protected $collection;

    /**
     * @var string
     */
    protected $aggregateRootClass;

    public function __construct(
        MongoDbClient $mongoDbClient,
        string $collectionName,
        string $aggregateRootClass,
        ObjectNormalizerInterface $normalizer
    )
    {
        if (!$collectionName) {
            throw new InvalidArgumentException('The name of the collection must be provided.');
        }

        if (!$aggregateRootClass) {
            throw new InvalidArgumentException('The class of the Aggregate Root must be provided.');
        }

        if (!class_exists($aggregateRootClass)) {
            throw new InvalidArgumentException(sprintf('The Aggregate Root class "%s" does not exist.', $aggregateRootClass));
        }

        $this->collection = $mongoDbClient->getCollection($collectionName)->withOptions([
            'typeMap' => [
                'document' => 'array',
                'root' => 'array',
            ]
        ]);

        $this->aggregateRootClass = $aggregateRootClass;

        $this->normalizer = $normalizer;
        $this->configureNormalizer($normalizer);
    }

    /**
     * Methods that allows subclasses to configure the normalizer according to their needs for
     * this repository.
     * @param ObjectNormalizerInterface $normalizer
     */
    protected function configureNormalizer(ObjectNormalizerInterface $normalizer): void
    {
    }

    /**
     * Adds an aggregate root to this repository.
     * @param $aggregateRoot
     */
    protected function addAggregateRoot($aggregateRoot): void
    {
        $data = $this->normalizeAggregateRoot($aggregateRoot);
        $this->collection->insertOne($data);
    }

    /**
     * Finds an Aggregate Root by its Id or returns null if it was not found.
     * @param string $id
     * @return mixed
     */
    protected function findAggregateRootById(string $id)
    {
        return $this->findOneAggregateRootBy(['_id' => $id]);
    }

    /**
     * Finds all aggregate roots that match a given set of filters
     * or an empty array if none matched.
     * @param array $filter
     * @return array
     */
    protected function findManyAggregateRootsBy(array $filter): array
    {
        $cursor = $this->collection->find($filter);
        $cursorResult = $cursor->toArray();

        $self = $this;

        return array_map(static function ($data) use ($self) {
            return $self->denormalizeAggregateRoot($data);
        }, $cursorResult);
    }

    /**
     * Finds one aggregate root that match a given set of filters or null
     * if none could be found.
     * @param array $filter
     * @return mixed
     */
    protected function findOneAggregateRootBy(array $filter)
    {
        $data = $this->collection->findOne($filter);

        return $this->denormalizeAggregateRoot($data);
    }

    /**
     * Returns all aggregate roots in this repository.
     * @return array
     */
    protected function findAllAggregateRoots(): array
    {
        return $this->findManyAggregateRootsBy([]);
    }

    /**
     * Updates an aggregate root in this repository.
     * @param string $id
     * @param $aggregateRoot
     */
    protected function updateAggregateRoot(string $id, $aggregateRoot): void
    {
        $arr = $this->normalizeAggregateRoot($aggregateRoot);
        $filter = [
            '_id' => $id,
        ];
        $this->collection->updateOne($filter, ['$set' => $arr]);
    }

    /**
     * Removes an aggregate root from this repository by its ID.
     * @param string $id
     */
    protected function removeAggregateRoot(string $id): void
    {
        $this->collection->deleteOne(['_id' => $id]);
    }

    /**
     * Denormalizes the normalized form of an aggregate root to an instance of its class
     * and returns it.
     * @return mixed
     */
    protected function denormalizeAggregateRoot(?array $data)
    {
        if ($data) {
            $data[$this->getAggregateRootIdDataKey()] = $data['_id'];
            unset($data['_id']);
        }

        return $this->normalizer->denormalize($data, $this->aggregateRootClass);
    }

    /**
     * Normalizes an aggregate root and returns its normalized form.
     */
    protected function normalizeAggregateRoot($aggregateRoot): array
    {
        $data = $this->normalizer->normalize($aggregateRoot);
        $data['_id'] = $data[$this->getAggregateRootIdDataKey()];
        unset($data[$this->getAggregateRootIdDataKey()]);

        return $data;
    }

    /**
     * Returns the name of the index in the data array containing the id
     * of the aggregate root.
     * Since MongoDB relies by default on an _id key which is different than
     * most aggregate root implementations ("id"), this method allows to define which key should be
     * used to be mapped to the _id key.
     * @return string
     */
    protected function getAggregateRootIdDataKey(): string
    {
        return 'id';
    }
}
