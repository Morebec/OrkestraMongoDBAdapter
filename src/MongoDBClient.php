<?php

namespace Morebec\Orkestra\Adapter\MongoDB;

use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Driver\ReadConcern;
use MongoDB\Driver\ReadPreference;
use MongoDB\Driver\Session;
use MongoDB\Driver\WriteConcern;

class MongoDBClient
{
    /**
     * @var Client
     */
    private $client;

    /**
     * @var string
     */
    private $databaseName;

    /**
     * @var Session
     */
    private $session;

    public function __construct(string $connectionString, string $databaseName)
    {
        $this->client = new Client($connectionString);
        $this->databaseName = $databaseName;
        $this->session = null;
    }

    public function getCollection(string $collectionName): Collection
    {
        return $this->client->{$this->databaseName}->{$collectionName};
    }

    public function startTransaction(): void
    {
        if ($this->session) {
            return;
        }

        $this->session = $this->client->startSession([
            'readPreference' => new ReadPreference(ReadPreference::RP_PRIMARY),
        ]);

        $this->session->startTransaction([
            'readConcern' => new ReadConcern('snapshot'),
            'writeConcern' => new WriteConcern(WriteConcern::MAJORITY),
        ]);
    }

    public function commitTransaction(): void
    {
        $this->session->endSession();
        $this->session = null;
    }

    public function rollbackTransaction(): void
    {
        $this->session->abortTransaction();
        $this->session = null;
    }
}
