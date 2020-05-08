<?php

namespace Morebec\Orkestra\Adapter\MongoDB;

use MongoDB\Client;
use MongoDB\Collection;

class MongoDBClient
{
    public function __construct(string $connectionString, string $databaseName)
    {
        $this->client = new Client($connectionString);
        $this->databaseName = $databaseName;
    }

    public function getCollection(string $collectionName): Collection
    {
        return $this->client->{$this->databaseName}->{$collectionName};
    }
}
