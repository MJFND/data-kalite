from dataclasses import dataclass


@dataclass
class MetaData:
    """
    Centralizing all metadata columns
    """

    id: str = "id"
    created_at: str = "created_at"
    pipeline_args: str = "pipeline_args"
    dag_id: str = "dag_id"
    dag_run_id: str = "dag_run_id"
    dag_task_id: str = "dag_task_id"
