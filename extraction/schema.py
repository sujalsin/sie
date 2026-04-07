from __future__ import annotations

from validation.schema import (
    ExtractedFields,
    coerce_flat_record,
    empty_extracted_fields,
)

coerce_extracted_fields = coerce_flat_record

__all__ = ["ExtractedFields", "coerce_extracted_fields", "empty_extracted_fields"]
