<?php

namespace Morebec\OrkestraMongoDbAdapter\Privacy;

use Morebec\Orkestra\DateTime\DateTime;
use Morebec\Orkestra\Privacy\PersonalRecordInterface;

/**
 * Implementation of a Personal Record Interface as Saved in the
 * Mongo DB implementation of the {@link MongoDbPersonalInformationStore}.
 */
class PersonalRecordDocument implements PersonalRecordInterface
{
    public const ID_KEY = '_id';
    public const PERSONAL_TOKEN_KEY = 'personalToken';
    public const KEY_NAME_KEY = 'keyName';
    public const SOURCE_KEY = 'source';
    public const REASONS_KEY = 'reasons';
    public const PROCESSING_REQUIREMENTS_KEY = 'processingRequirements';
    public const DISPOSED_AT_KEY = 'disposedAt';
    public const METADATA_KEY = 'metadata';
    public const COLLECTED_AT_KEY = 'collectedAt';
    public const VALUE_KEY = 'value';

    /**
     * @var array
     */
    private $data;

    /**
     * StoredPersonalRecord constructor.
     */
    public function __construct(array $data)
    {
        $this->data = $data;
    }

    public function getId(): string
    {
        return $this->data[self::ID_KEY];
    }

    public function getPersonalToken(): string
    {
        return $this->data[self::PERSONAL_TOKEN_KEY];
    }

    public function getKeyName(): string
    {
        return $this->data[self::KEY_NAME_KEY];
    }

    public function getSource(): string
    {
        return $this->data[self::SOURCE_KEY];
    }

    public function getReasons(): array
    {
        return $this->data[self::REASONS_KEY];
    }

    public function getProcessingRequirements(): array
    {
        return $this->data[self::PROCESSING_REQUIREMENTS_KEY];
    }

    public function getDisposedAt(): ?DateTime
    {
        $d = $this->data[self::DISPOSED_AT_KEY];
        return DateTime::createFromFormat('U.u', $d);
    }

    public function getMetadata(): array
    {
        return $this->data[self::METADATA_KEY];
    }

    public function getCollectedAt(): ?DateTime
    {
        $d = $this->data[self::COLLECTED_AT_KEY];
        return DateTime::createFromFormat('U.u', $d);
    }

    public function getValue()
    {
        return $this->data[self::VALUE_KEY];
    }
}
