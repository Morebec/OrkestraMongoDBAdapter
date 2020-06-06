<?php

namespace Test\Morebec\Orkestra\Adapter\MongoDB\EventPersistence;

use Morebec\Orkestra\Adapter\MongoDB\EventPersistence\EventTypeRegistry;
use PHPUnit\Framework\TestCase;

class EventTypeRegistryTest extends TestCase
{
    public function testUnregisterEventType()
    {
        $registry = new EventTypeRegistry();
        $registry->registerEventType(self::class);
        $registry->unregisterEventType(self::class);
        $this->assertFalse($registry->isEventTypeRegistered(self::class));
    }

    public function testGetMap()
    {
        $registry = new EventTypeRegistry();
        $registry->registerEventType(self::class);
        $this->assertEquals([
            'EventTypeRegistryTest' => self::class,
        ], $registry->getMap());
    }

    public function testResolveFQN()
    {
        $registry = new EventTypeRegistry();
        $registry->registerEventType(self::class);
        $this->assertEquals(self::class, $registry->resolveFQN('EventTypeRegistryTest'));
    }

    public function testRegisterEventType()
    {
        $registry = new EventTypeRegistry();
        $registry->registerEventType(self::class);
        $this->assertTrue($registry->isEventTypeRegistered(self::class));
    }

    public function testIsEventTypeRegistered()
    {
        $registry = new EventTypeRegistry();
        $registry->registerEventType(self::class);
        $this->assertTrue($registry->isEventTypeRegistered(self::class));

        $this->assertFalse($registry->isEventTypeRegistered(EventTypeRegistry::class));
    }
}
