from __future__ import annotations

from airflow.models import BaseOperator

from quality.validator import run_layer_validation


class DataQualityOperator(BaseOperator):
    template_fields = ("layer", "table_name", "run_id")

    def __init__(self, layer: str, table_name: str | None = None, run_id: str | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.layer = layer
        self.table_name = table_name
        self.run_id = run_id

    def execute(self, context):
        return run_layer_validation(self.layer, table_name=self.table_name, run_id=self.run_id)

