from gemma.training.config import TASK_SYSTEM_PROMPTS, TaskType
from gemma.training.datasets import build_dataset


def test_hermes_operator_is_first_class_training_task():
    assert TaskType.HERMES_OPERATOR.value == "hermes_operator"
    assert "GRID Hermes" in TASK_SYSTEM_PROMPTS[TaskType.HERMES_OPERATOR]

    rows = build_dataset(TaskType.HERMES_OPERATOR, shuffle=False)

    assert rows
    conversation = rows[0]["conversations"]
    assert conversation[0]["role"] == "system"
    assert conversation[1]["role"] == "user"
    assert conversation[2]["role"] == "assistant"
    assert "read-only fleet audit" in conversation[2]["content"]
