from __future__ import annotations

from airflow.models import BaseOperator


class DeltaCommandOperator(BaseOperator):
    template_fields = ("callable_path", "kwargs")

    def __init__(self, callable_path: str, kwargs: dict | None = None, **operator_kwargs) -> None:
        super().__init__(**operator_kwargs)
        self.callable_path = callable_path
        self.kwargs = kwargs or {}

    def execute(self, context):
        module_name, function_name = self.callable_path.rsplit(".", 1)
        module = __import__(module_name, fromlist=[function_name])
        callable_obj = getattr(module, function_name)
        return callable_obj(**self.kwargs)

