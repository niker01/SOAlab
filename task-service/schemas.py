from typing import Optional
from datetime import datetime
from pydantic import BaseModel
from pydantic import field_validator

def validate_datetime(value: Optional[str]) -> Optional[str]:
    if value is not None:
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("Date must be an ISO 8601 datetime") from exc
    return value

class TaskListCreate(BaseModel):
    title: str

class TaskListUpdate(BaseModel):
    title: str

class TaskCreate(BaseModel):
    title: str
    description: Optional[str] = None
    list_id: Optional[str] = None
    due_date: Optional[str] = None

    @field_validator("due_date")
    @classmethod
    def validate_due_date(cls, value: Optional[str]) -> Optional[str]:
        return validate_datetime(value)

class TaskUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    due_date: Optional[str] = None

    @field_validator("due_date")
    @classmethod
    def validate_due_date(cls, value: Optional[str]) -> Optional[str]:
        return validate_datetime(value)

class TaskAssignList(BaseModel):
    list_id: str