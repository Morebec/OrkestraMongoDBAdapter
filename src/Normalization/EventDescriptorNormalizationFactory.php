<?php

namespace Morebec\Orkestra\Adapter\MongoDB\Normalization;

use DateTime as PHPDateTime;
use Morebec\DateTime\DateTime;
use Morebec\DomainNormalizer\Denormalization\Configuration\DenormalizationDefinition;
use Morebec\DomainNormalizer\Denormalization\Configuration\ObjectDenormalizationDefinition;
use Morebec\DomainNormalizer\Denormalization\DenormalizationContext;
use Morebec\DomainNormalizer\Normalization\Configuration\NormalizationDefinition;
use Morebec\DomainNormalizer\Normalization\Configuration\ObjectNormalizationDefinition;
use Morebec\DomainNormalizer\Normalization\NormalizationContext;
use Morebec\Orkestra\EventSourcing\EventStore\EventDescriptor;
use Morebec\Orkestra\Messaging\Event\EventInterface;

class EventDescriptorNormalizationFactory
{
    /**
     * Returns the normalization definition for events.
     */
    public static function getNormalizationDefinition(): NormalizationDefinition
    {
        $def = new ObjectNormalizationDefinition(EventDescriptor::class);
        $def->property('eventId')->renamedTo('_id');

        // $def->property('stream')->asString();
        // $def->property('version');
        $def->property('payload')->asTransformed(EventInterface::class);
        $def->property('occurredAt')->as(static function (NormalizationContext $context) {
            /** @var DateTime $v */
            $v = $context->getValue();

            return (float) $v->format('U.u');
        });
        $def->property('eventType');

        return $def;
    }

    /**
     * Returns the normalization definition for events.
     */
    public static function getDenormalizationDefinition(): DenormalizationDefinition
    {
        $def = new ObjectDenormalizationDefinition(EventDescriptor::class);

        $def->key('_id')->renamedTo('eventId');
        /*        $def->key('aggregateId')->as(static function (DenormalizationContext $context) {
                    return new AggregateRootId($context->getValue());
                });*/
        // $def->key('version');
        $def->key('payload')->asTransformed(EventInterface::class);
        $def->key('occurredAt')->as(static function (DenormalizationContext $context) {
            $dt = PHPDateTime::createFromFormat('U.u', $context->getValue());

            return DateTime::fromPHPDateTime($dt);
        });
        $def->key('eventType')->defaultValue(null);

        return $def;
    }
}
