from sqlalchemy import text
from src.db import engine
 
 
def get_tasks():
    try:
        print("get_tasks() called")
        with engine.connect() as conn:
            print("Connected to DB")
            result = conn.execute(text("SELECT * FROM tasks"))
            print("Query executed")
            return [dict(row._mapping) for row in result]
    except Exception as e:
        print(" Error in get_tasks():", e)
        raise
 
def get_task_by_id(task_id):
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM tasks WHERE id = :id"), {"id": task_id})
            row = result.fetchone()
            return dict(row._mapping) if row else None
    except Exception as e:
        print("Error in get_task_by_id:", e)
        raise
 
 
 
def create_task(data):
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO tasks (title, description, status) VALUES (:title, :desc, :status)"),
            {"title": data.title, "desc": data.description, "status": data.status}
        )
 
def update_task(id, data):
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE tasks SET title=:title, description=:desc, status=:status, updated_at=GETDATE() WHERE id=:id"),
            {"id": id, "title": data.title, "desc": data.description, "status": data.status}
        )
 
def delete(id):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM tasks WHERE id=:id"), {"id": id})