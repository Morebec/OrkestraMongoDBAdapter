<?php


namespace Morebec\OrkestraMongoDbAdapter\Privacy;

use MongoDB\Collection;
use MongoDB\Driver\Session;
use Morebec\Orkestra\DateTime\ClockInterface;
use Morebec\Orkestra\Privacy\PersonalInformationStoreInterface;
use Morebec\Orkestra\Privacy\PersonalRecordInterface;
use Morebec\OrkestraMongoDbAdapter\MongoDbClient;

/**
 * MongoDB implementation of a Personal Information Store..
 */
class MongoDbPersonalInformationStore implements PersonalInformationStoreInterface
{
    public const DEFAULT_COLLECTION_NAME = 'pii.store';
    /**
     * @var MongoDbClient
     */
    private $mongoDbClient;

    /**
     * @var string
     */
    private $collectionName;

    /**
     * @var Collection
     */
    private $collection;

    /**
     * @var Session|null
     */
    private $session;
    /**
     * @var ClockInterface
     */
    private $clock;


    public function __construct(
        ClockInterface $clock,
        MongoDbClient $mongoDbClient,
        string $collectionName = self::DEFAULT_COLLECTION_NAME
    ) {
        $this->mongoDbClient = $mongoDbClient;
        $this->collectionName = $collectionName;
        $this->collection = $this->mongoDbClient->getCollection($collectionName);
        $this->session = $this->mongoDbClient->getSession();
        $this->clock = $clock;
    }

    public function put(PersonalRecordInterface $record): void
    {
        $document = [
            PersonalRecordDocument::ID_KEY => $record->getId(),
            PersonalRecordDocument::KEY_NAME_KEY => $record->getKeyName(),
            PersonalRecordDocument::PERSONAL_TOKEN_KEY => $record->getPersonalToken(),
            PersonalRecordDocument::VALUE_KEY => $record->getValue(),
            PersonalRecordDocument::REASONS_KEY => $record->getReasons(),
            PersonalRecordDocument::PROCESSING_REQUIREMENTS_KEY => $record->getProcessingRequirements(),
            PersonalRecordDocument::METADATA_KEY => $record->getMetadata(),
            PersonalRecordDocument::SOURCE_KEY => $record->getSource(),
            PersonalRecordDocument::COLLECTED_AT_KEY => $record->getCollectedAt() ?: (float)$this->clock->now()->format('U.u'),
            PersonalRecordDocument::DISPOSED_AT_KEY => $record->getDisposedAt() ? (float)$record->getDisposedAt()->format('U.u'): null,
        ];
        $this->collection->insertOne($document, ['session' => $this->session]);
    }

    public function findOneByKeyName(string $personalToken, string $keyName): ?PersonalRecordInterface
    {
        $result = $this->collection->findOne(
            [
                PersonalRecordDocument::PERSONAL_TOKEN_KEY => $personalToken,
                PersonalRecordDocument::KEY_NAME_KEY => $keyName
            ],
            ['typeMap' => $this->getTypeMapOption()]
        );

        if (!$result) {
            return null;
        }

        return $this->convertDocumentToRecord($result);
    }

    public function findById(string $personalRecordId): ?PersonalRecordInterface
    {
        $result = $this->collection->findOne(
            [PersonalRecordDocument::ID_KEY => $personalRecordId],
            ['typeMap' => $this->getTypeMapOption()]
        );

        if (!$result) {
            return null;
        }

        return $this->convertDocumentToRecord($result);
    }

    public function findByPersonalToken(string $personalToken): iterable
    {
        $results = $this->collection->findOne(
            [PersonalRecordDocument::PERSONAL_TOKEN_KEY => $personalToken],
            ['typeMap' => $this->getTypeMapOption()]
        );


        foreach ($results as $result) {
            yield $this->convertDocumentToRecord($result);
        }
    }

    public function remove(string $personalRecordId): void
    {
        $this->collection->deleteOne(
            [PersonalRecordDocument::ID_KEY => $personalRecordId],
            ['session' => $this->session]
        );
    }

    public function erase(string $personalToken): void
    {
        $this->collection->deleteMany(
            [PersonalRecordDocument::PERSONAL_TOKEN_KEY => $personalToken],
            ['session' => $this->session]
        );
    }

    /**
     * @return string[]
     */
    private function getTypeMapOption(): array
    {
        return [
            'document' => 'array',
            'root' => 'array',
        ];
    }

    /**
     * Converts a MongoDb document to a PersonalRecordInterface.
     * @param array $result
     * @return PersonalRecordInterface
     */
    protected function convertDocumentToRecord(array $result): PersonalRecordInterface
    {
        return new PersonalRecordDocument($result);
    }
}
