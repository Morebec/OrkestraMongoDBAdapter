<?php

/** @noinspection SqlNoDataSourceInspection */

namespace Morebec\Orkestra\Adapter\MongoDB;

use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ODM\MongoDB\LockException;
use Doctrine\ODM\MongoDB\Mapping\ClassMetadata;
use Doctrine\ODM\MongoDB\MongoDBException;
use Doctrine\ODM\MongoDB\Query\Expr;
use Doctrine\Persistence\ObjectRepository;
use LogicException;
use Morebec\Orkestra\Adapter\MongoDB\Document\WorkflowStateDocument;
use Morebec\Orkestra\Workflow\Query\ExpressionNode;
use Morebec\Orkestra\Workflow\Query\ExpressionOperator;
use Morebec\Orkestra\Workflow\Query\Query;
use Morebec\Orkestra\Workflow\Query\TermNode;
use Morebec\Orkestra\Workflow\Query\TermOperator;
use Morebec\Orkestra\Workflow\WorkflowState;
use Morebec\Orkestra\Workflow\WorkflowStateRepositoryInterface;

class MongoDBWorkflowStateRepository implements WorkflowStateRepositoryInterface
{
    /**
     * @var DocumentManager
     */
    private $documentManager;

    public function __construct(DocumentManager $documentManager)
    {
        $this->documentManager = $documentManager;
    }

    /**
     * {@inheritdoc}
     *
     * @throws MongoDBException
     */
    public function add(WorkflowState $state): void
    {
        $doc = WorkflowStateDocument::fromWorkflowState($state);
        $this->documentManager->persist($doc);
        $this->documentManager->flush();
    }

    /**
     * {@inheritdoc}
     *
     * @throws LockException
     * @throws MongoDBException
     */
    public function update(WorkflowState $state): void
    {
        $doc = WorkflowStateDocument::fromWorkflowState($state);
        $doc = $this->documentManager->merge($doc);
        $this->documentManager->persist($doc);
        $this->documentManager->flush();
    }

    /**
     * {@inheritdoc}
     *
     * @throws LockException
     * @throws MongoDBException
     */
    public function remove(WorkflowState $state): void
    {
        $doc = WorkflowStateDocument::fromWorkflowState($state);
        $doc = $this->documentManager->merge($doc);
        $this->documentManager->remove($doc);
        $this->documentManager->flush();
    }

    /**
     * {@inheritdoc}
     */
    public function findById(string $id): ?WorkflowState
    {
        /** @var WorkflowStateDocument|null $doc */
        $doc = $this->getDocumentRepository()->find($id);
        if (!$doc) {
            return null;
        }

        return $doc->toWorkflowState();
    }

    /**
     * {@inheritdoc}
     */
    public function findByWorkflowId(string $workflowId): array
    {
        /** @var WorkflowStateDocument[] $docs */
        $docs = $this->getDocumentRepository()->findBy(['workflowId' => $workflowId]);

        return array_map(static function (WorkflowStateDocument $d) {
            return $d->toWorkflowState();
        }, $docs);
    }

    /**
     * {@inheritdoc}
     */
    public function findBy(Query $query): array
    {
        $expr = $this->convertExprNodeToExpr($query->getExpressionNode());
        $docs = $this->getDocumentRepository()->findBy($expr->getQuery());

        return array_map(static function (WorkflowStateDocument $d) {
            return $d->toWorkflowState();
        }, $docs);
    }

    /**
     * {@inheritdoc}
     */
    public function findOneBy(Query $query): ?WorkflowState
    {
        $expr = $this->convertExprNodeToExpr($query->getExpressionNode());
        $criteria = $expr->getQuery();
        /* @var WorkflowStateDocument|null $doc */
        $doc = $this->getDocumentRepository()->findOneBy($criteria);

        return $doc ? $doc->toWorkflowState() : null;
    }

    /**
     * Converts a Workflow Query expression to a doctrine ODM expression.
     */
    private function convertExprNodeToExpr(ExpressionNode $node): Expr
    {
        if ($node instanceof TermNode) {
            return $this->convertTermToExpr($node);
        }

        $leftNode = $node->getLeft();
        $leftValue = $this->convertExprNodeToExpr($leftNode);

        $operator = $node->getOperator();
        if (!$operator) {
            return $leftValue;
        }

        /** @var ExpressionNode $rightNode */
        $rightNode = $node->getRight();
        $rightValue = $this->convertExprNodeToExpr($rightNode);

        $expr = new Expr($this->documentManager);
        $expr->setClassMetadata(new ClassMetadata(WorkflowStateDocument::class));
        if ($operator->isEqualTo(ExpressionOperator::OR())) {
            return $expr->addOr($leftValue, $rightValue);
        }

        return $expr->addAnd($leftValue, $rightValue);
    }

    /**
     * Converts a Term node to a ODM Expression.
     */
    private function convertTermToExpr(TermNode $node): Expr
    {
        $expr = new Expr($this->documentManager);
        $expr->setClassMetadata(new ClassMetadata(WorkflowStateDocument::class));

        $field = $node->getField();

        if ($field === 'workflow_id') {
            $field = 'workflowId';
        }

        if (!\in_array($field, ['id', 'workflowId', 'completed'])) {
            $field = "data.$field";
        }

        $expr->field($field);

        $value = $node->getValue();
        $termOperator = $node->getTermOperator();
        switch ($termOperator) {
            case TermOperator::CONTAINS: // Although that appears counter intuitive, MongoDB works that way with arrays
            case TermOperator::EQUAL:
                return $expr->equals($value);

            case TermOperator::NOT_CONTAINS: // Although that appears counter intuitive, MongoDB works that way with arrays
            case TermOperator::NOT_EQUAL:
                return $expr->notEqual($value);

            case TermOperator::LESS_THAN:
                return $expr->lt($value);

            case TermOperator::GREATER_THAN:
                return $expr->gt($value);
            case TermOperator::LESS_OR_EQUAL:
                return $expr->lte($value);
            case TermOperator::GREATER_OR_EQUAL:
                return $expr->gte($value);
            case TermOperator::IN:
                return $expr->in($value);
            case TermOperator::NOT_IN:
                return $expr->notIn($value);
        }

        throw new LogicException("Unsupported Operator {$termOperator}");
    }

    private function getDocumentRepository(): ObjectRepository
    {
        return $this->documentManager->getRepository(WorkflowStateDocument::class);
    }
}
