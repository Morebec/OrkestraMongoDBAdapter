<?php

namespace Morebec\Orkestra\Adapter\MongoDB;

use MongoDB\Collection;
use Morebec\DomainNormalizer\Denormalization\Configuration\DenormalizerConfiguration;
use Morebec\DomainNormalizer\Denormalization\Configuration\ObjectDenormalizationDefinition;
use Morebec\DomainNormalizer\Denormalization\Configuration\ObjectDenormalizationDefinitionFactory;
use Morebec\DomainNormalizer\Denormalization\Denormalizer;
use Morebec\DomainNormalizer\Normalization\Configuration\NormalizerConfiguration;
use Morebec\DomainNormalizer\Normalization\Configuration\ObjectNormalizationDefinition;
use Morebec\DomainNormalizer\Normalization\Configuration\ObjectNormalizationDefinitionFactory;
use Morebec\DomainNormalizer\Normalization\Normalizer;
use Morebec\Orkestra\EventSourcing\EventStore\EventStoreTrackingUnit;
use Morebec\Orkestra\EventSourcing\EventStore\EventStoreTrackingUnitRepositoryInterface;

class MongoDBEventStoreTrackingUnitRepository implements EventStoreTrackingUnitRepositoryInterface
{
    private const COLLECTION_NAME = 'event_store_tracking_units';

    /**
     * @var Collection
     */
    private $collection;

    /**
     * @var Normalizer
     */
    private $normalizer;

    /**
     * @var Denormalizer
     */
    private $denormalizer;

    public function __construct(MongoDBClient $client)
    {
        $this->collection = $client->getCollection(self::COLLECTION_NAME);

        $normConfig = new NormalizerConfiguration();
        $normConfig->registerDefinition(ObjectNormalizationDefinitionFactory::forClass(
            EventStoreTrackingUnit::class,
            static function (ObjectNormalizationDefinition $d) {
                return $d->property('id')->renamedTo('_id')
                         ->property('lastReadEventId')
                ;
            }
        ));
        $this->normalizer = new Normalizer($normConfig);

        $denormConfig = new DenormalizerConfiguration();
        $denormConfig->registerDefinition(ObjectDenormalizationDefinitionFactory::forClass(
            EventStoreTrackingUnit::class,
            static function (ObjectDenormalizationDefinition $d) {
                $d->key('_id')->renamedTo('id');
                $d->key('lastReadEventId');

                return $d;
            }
        ));
        $this->denormalizer = new Denormalizer($denormConfig);
    }

    /**
     * {@inheritdoc}
     */
    public function update(EventStoreTrackingUnit $trackingUnit)
    {
        $data = $this->normalizer->normalize($trackingUnit);

        $filter = ['_id' => $data['_id']];
        $update = ['$set' => $data];
        $options = ['upsert' => true];

        $this->collection->updateOne($filter, $update, $options);
    }

    /**
     * {@inheritdoc}
     */
    public function findById(string $trackingUnitId): ?EventStoreTrackingUnit
    {
        $filter = ['_id' => $trackingUnitId];
        $options = [
            // Convert BSON documents to PHP Assoc Array
            'typeMap' => [
                'document' => 'array',
                'root' => 'array',
            ],
        ];

        $doc = $this->collection->findOne($filter, $options);
        if (!$doc) {
            return null;
        }

        /** @var EventStoreTrackingUnit $unit */
        $unit = $this->denormalizer->denormalize($doc, EventStoreTrackingUnit::class);

        return $unit;
    }
}
