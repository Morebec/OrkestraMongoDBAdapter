<?php

namespace Morebec\Orkestra\Adapter\MongoDB\Document;

use Doctrine\ODM\MongoDB\Mapping\Annotations as ODM;
use Morebec\Orkestra\Workflow\WorkflowState;

/**
 * @ODM\Document(collection="workflow_states")
 * @ODM\UniqueIndex(keys={"id"="asc"})
 */
class WorkflowStateDocument
{
    /**
     * @ODM\Id(type="string", strategy="NONE")
     *
     * @var string
     */
    public $id;

    /**
     * @ODM\Field(type="string")
     *
     * @var string
     */
    public $workflowId;

    /**
     * @ODM\Field(type="bool")
     *
     * @var bool
     */
    public $completed;

    /**
     * @ODM\Field(type="hash")
     *
     * @var array<string, mixed>
     */
    public $data;

    final public function __construct()
    {
    }

    /**
     * Creates a doc instance from a workflow state.
     *
     * @return static
     */
    public static function fromWorkflowState(WorkflowState $state): self
    {
        $data = $state->toArray();

        $doc = new static();
        $doc->id = $data['id'];
        $doc->workflowId = $data['workflow_id'];
        $doc->completed = $data['completed'];
        $doc->data = $data['data'];

        return $doc;
    }

    /**
     * Converts this Document to a Workflow state.
     */
    public function toWorkflowState(): WorkflowState
    {
        $data = [
            'id' => $this->id,
            'workflow_id' => $this->workflowId,
            'completed' => $this->completed,
            'data' => $this->data,
        ];

        return WorkflowState::fromArray($data);
    }
}
