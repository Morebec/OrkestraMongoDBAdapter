<?php

namespace Morebec\OrkestraMongoDbAdapter\Messaging;

use Morebec\Orkestra\Messaging\DomainMessageHeaders;
use Morebec\Orkestra\Messaging\DomainMessageInterface;
use Morebec\Orkestra\Messaging\DomainResponseInterface;
use Morebec\Orkestra\Messaging\Middleware\DomainMessageBusMiddlewareInterface;
use Morebec\OrkestraMongoDbAdapter\MongoDbClient;
use Psr\Log\LoggerInterface;
use Throwable;

/**
 * Wraps the bus with Mongo DB Transactions.
 */
class MongoDbTransactionMiddleware implements DomainMessageBusMiddlewareInterface
{
    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var MongoDbClient
     */
    private $mongoDbClient;

    /**
     * Used to track if the current domain message bus is a top level call or a nested call.
     * @var string|null
     */
    private $topLevelMessageId;


    public function __construct(LoggerInterface $logger, MongoDbClient $mongoDbClient)
    {
        $this->logger = $logger;
        $this->mongoDbClient = $mongoDbClient;
        $this->topLevelMessageId = null;
    }

    public function handle(DomainMessageInterface $domainMessage, DomainMessageHeaders $headers, callable $next): DomainResponseInterface
    {
        $messageId = $headers->get(DomainMessageHeaders::MESSAGE_ID);

        if (!$this->topLevelMessageId) {
            $this->topLevelMessageId = $messageId;
        }

        try {
            $this->startTransaction($messageId);
            $response = $next($domainMessage, $headers);
            $this->commitTransaction($messageId);
            return $response;
        } catch (Throwable $throwable) {
            $this->rollbackTransaction($messageId);
            throw $throwable;
        }
    }

    private function startTransaction(string $messageId): void
    {
        if (!$this->canHandleTransaction($messageId)) {
            return;
        }

        $this->mongoDbClient->startTransaction();
    }

    private function commitTransaction(string $messageId): void
    {
        if (!$this->canHandleTransaction($messageId)) {
            return;
        }

        $this->mongoDbClient->commitTransaction();
    }

    protected function rollbackTransaction(string $messageId): void
    {
        if (!$this->canHandleTransaction($messageId)) {
            return;
        }

        $this->mongoDbClient->rollbackTransaction();
    }

    /**
     * @param string $messageId
     * @return bool
     */
    private function canHandleTransaction(string $messageId): bool
    {
        return $this->topLevelMessageId === $messageId && $this->mongoDbClient->getSession();
    }
}
