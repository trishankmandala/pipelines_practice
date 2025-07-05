from pydantic import BaseModel
from datetime import datetime
from typing import Optional
 
class Task(BaseModel):
    id: Optional[int] = None
    title: str
    description: str
    status: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None