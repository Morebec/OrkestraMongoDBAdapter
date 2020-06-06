<?php

namespace Morebec\Orkestra\Adapter\MongoDB\EventPersistence;

use Morebec\DateTime\DateTime;
use Morebec\DomainNormalizer\Denormalization\Configuration\AutomaticDenormalizationDefinition;
use Morebec\DomainNormalizer\Denormalization\Configuration\DenormalizationDefinition;
use Morebec\DomainNormalizer\Denormalization\DenormalizationContext;
use Morebec\DomainNormalizer\Normalization\Configuration\AutomaticNormalizationDefinition;
use Morebec\DomainNormalizer\Normalization\Configuration\NormalizationDefinition;
use Morebec\DomainNormalizer\Normalization\NormalizationContext;
use Morebec\Orkestra\Messaging\Event\EventInterface;

class EventNormalizationFactory
{
    /**
     * Returns the normalization definition for events.
     */
    public static function getNormalizationDefinition(): NormalizationDefinition
    {
        $def = new AutomaticNormalizationDefinition(EventInterface::class);
        // $def->property('id');
        $def->property('occurredAt')->as(static function (NormalizationContext $context) {
            $value = $context->getValue();
            /* @var DateTime $value */
            return (float) $value->format('U.u');
        });

        return $def;
    }

    /**
     * Returns the normalization definition for events.
     */
    public static function getDenormalizationDefinition(): DenormalizationDefinition
    {
        $def = new AutomaticDenormalizationDefinition(EventInterface::class);
        // $def->key('id');
        $def->key('occurredAt')->as(static function (DenormalizationContext $context) {
            $dt = \DateTime::createFromFormat('U.u', $context->getValue());

            return DateTime::fromPHPDateTime($dt);
        });

        return $def;
    }
}
