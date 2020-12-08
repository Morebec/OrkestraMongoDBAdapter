<?php

namespace Morebec\OrkestraMongoDbAdapter;

use InvalidArgumentException;
use Morebec\Orkestra\Normalization\ObjectNormalizerInterface;

/**
 * Simple Abstract implementation of a State based Aggregate Root Repository.
 */
abstract class AbstractMongoDbAggregateRootRepository extends AbstractMongoDbObjectStore
{
    public function __construct(
        MongoDbClient $mongoDbClient,
        string $collectionName,
        string $aggregateRootClass,
        ObjectNormalizerInterface $normalizer
    ) {
        if (!$collectionName) {
            throw new InvalidArgumentException('The name of the collection must be provided.');
        }

        if (!$aggregateRootClass) {
            throw new InvalidArgumentException('The class of the Aggregate Root must be provided.');
        }

        if (!class_exists($aggregateRootClass)) {
            throw new InvalidArgumentException(sprintf('The Aggregate Root class "%s" does not exist.', $aggregateRootClass));
        }

        parent::__construct($mongoDbClient, $collectionName, $aggregateRootClass, $normalizer);
    }
}
