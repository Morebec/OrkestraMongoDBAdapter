<?php

namespace Morebec\Orkestra\Adapter\MongoDB\EventPersistence;

use InvalidArgumentException;
use ReflectionClass;
use ReflectionException;

class EventTypeRegistry
{
    public function __construct()
    {
        $this->map = [];
    }

    public function preload(array $data): void
    {
        $this->map = $data;
    }

    /**
     * Registeres an event type.
     *
     * @throws ReflectionException
     */
    public function registerEventType(string $eventFqn): void
    {
        $shortName = $this->getShortName($eventFqn);
        $this->map[$shortName] = $eventFqn;
    }

    /**
     * Unregisteres a given event type.
     *
     * @throws ReflectionException
     */
    public function unregisterEventType(string $eventFqn): void
    {
        if (!$this->isEventTypeRegistered($eventFqn)) {
            throw new InvalidArgumentException('Event $eventFqn was not registered');
        }
        $shortName = $this->getShortName($eventFqn);
        unset($this->map[$shortName]);
    }

    /**
     * Indicates if a given event type was registered or not.
     */
    public function isEventTypeRegistered(string $eventFqn): bool
    {
        $shortName = $this->getShortName($eventFqn);

        return array_key_exists($shortName, $this->map);
    }

    /**
     * @return string
     *
     * @throws InvalidArgumentException
     */
    public function resolveFQN(string $shortName): string
    {
        if (!array_key_exists($shortName, $this->map)) {
            throw new InvalidArgumentException("Cannot resolve $shortName");
        }

        return $this->map[$shortName];
    }

    public function getMap(): array
    {
        return $this->map;
    }

    /**
     * @throws ReflectionException
     */
    protected function getShortName(string $eventFqn): string
    {
        $r = new ReflectionClass($eventFqn);
        $shortName = $r->getShortName();

        return $shortName;
    }
}
