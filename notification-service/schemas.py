from pydantic import BaseModel
from pydantic import field_validator
from typing import Optional
from datetime import datetime

def validate_datetime(value: Optional[str]) -> Optional[str]:
    if value is not None:
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("Date must be an ISO 8601 datetime") from exc
    return value

class ReminderCreate(BaseModel):
    task_id: str
    reminder_time: str
    is_active: bool = True

    @field_validator("reminder_time")
    @classmethod
    def validate_reminder_time(cls, value: str) -> str:
        return validate_datetime(value)

class ReminderUpdate(BaseModel):
    reminder_time: Optional[str] = None
    status: Optional[str] = None
    is_active: Optional[bool] = None

    @field_validator("reminder_time")
    @classmethod
    def validate_reminder_time(cls, value: Optional[str]) -> Optional[str]:
        return validate_datetime(value)

class NotificationSend(BaseModel):
    user_id: str
    message: str
    channel: str = "push"