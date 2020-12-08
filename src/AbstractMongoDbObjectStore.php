<?php

namespace Morebec\OrkestraMongoDbAdapter;

use InvalidArgumentException;
use MongoDB\Collection;
use Morebec\Orkestra\Normalization\ObjectNormalizerInterface;

/**
 * Simple Abstract implementation of a State based Object Store.
 * It allows to store POPO in MongoDB easily using an {@link ObjectNormalizerInterface}
 */
abstract class AbstractMongoDbObjectStore
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
    protected $objectClassName;

    public function __construct(
        MongoDbClient $mongoDbClient,
        string $collectionName,
        string $objectClassName,
        ObjectNormalizerInterface $normalizer
    ) {
        if (!$collectionName) {
            throw new InvalidArgumentException('The name of the collection must be provided.');
        }

        if (!$objectClassName) {
            throw new InvalidArgumentException('The class of the Object class name must be provided.');
        }

        if (!class_exists($objectClassName)) {
            throw new InvalidArgumentException(sprintf('The Object class "%s" does not exist.', $objectClassName));
        }

        $this->collection = $mongoDbClient->getCollection($collectionName)->withOptions([
            'typeMap' => [
                'document' => 'array',
                'root' => 'array',
            ]
        ]);

        $this->objectClassName = $objectClassName;

        $this->normalizer = $normalizer;
        $this->configureNormalizer($normalizer);
    }

    /**
     * Methods that allows subclasses to configure the normalizer according to their needs for
     * this storage.
     * @param ObjectNormalizerInterface $normalizer
     */
    protected function configureNormalizer(ObjectNormalizerInterface $normalizer): void
    {
    }

    /**
     * Adds an object to this repository.
     * @param $o
     */
    protected function addObject($o): void
    {
        $data = $this->normalizeObject($o);
        $this->collection->insertOne($data);
    }

    /**
     * Finds an Object by its Id or returns null if it was not found.
     * @param string $id
     * @return mixed
     */
    protected function findObjectById(string $id)
    {
        return $this->findOneObjectBy(['_id' => $id]);
    }

    /**
     * Finds all objects that match a given set of filters
     * or an empty array if none matched.
     * @param array $filter
     * @return array
     */
    protected function findManyObjectsBy(array $filter): array
    {
        $cursor = $this->collection->find($filter);

        $objects = [];
        foreach ($cursor as $data) {
            $objects[] = $this->denormalizeObject($data);
        }
        return $objects;
    }

    /**
     * Finds one object that match a given set of filters or null
     * if none could be found.
     * @param array $filter
     * @return mixed
     */
    protected function findOneObjectBy(array $filter)
    {
        $data = $this->collection->findOne($filter);

        return $this->denormalizeObject($data);
    }

    /**
     * Returns all objects in this repository.
     * @return array
     */
    protected function findAllObjects(): array
    {
        return $this->findManyObjectsBy([]);
    }

    /**
     * Updates an object in this repository.
     * @param string $id
     * @param $object
     */
    protected function updateObject(string $id, $object): void
    {
        $arr = $this->normalizeObject($object);
        $filter = [
            '_id' => $id,
        ];
        $this->collection->updateOne($filter, ['$set' => $arr]);
    }

    /**
     * Removes an object from this repository by its ID.
     * @param string $id
     */
    protected function removeObject(string $id): void
    {
        $this->collection->deleteOne(['_id' => $id]);
    }

    /**
     * Denormalizes the normalized form of an object to an instance of its class
     * and returns it.
     * @return mixed
     */
    protected function denormalizeObject(?array $data)
    {
        if ($data) {
            $data[$this->getObjectIdDataKey()] = $data['_id'];
            unset($data['_id']);
        }

        return $this->normalizer->denormalize($data, $this->objectClassName);
    }

    /**
     * Normalizes an object and returns its normalized form.
     */
    protected function normalizeObject($o): array
    {
        $data = $this->normalizer->normalize($o);
        $data['_id'] = $data[$this->getObjectIdDataKey()];
        unset($data[$this->getObjectIdDataKey()]);

        return $data;
    }

    /**
     * Returns the name of the index in the normalized form array containing the id
     * of the object.
     * Since MongoDB relies by default on an _id key which is different than
     * most object implementations ("id"), this method allows to define which key should be
     * used to be mapped to the _id key.
     * @return string
     */
    protected function getObjectIdDataKey(): string
    {
        return 'id';
    }
}
