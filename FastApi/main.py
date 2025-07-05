from fastapi import FastAPI,HTTPException
from src.models import Task
from src.crud import get_tasks,create_task,update_task,delete,get_task_by_id
 
app=FastAPI()
 
@app.get("/")
def home():
    return {"message": "FastAPI is working!"}
 
 
@app.get("/tasks")
def read_tasks():
    return get_tasks()
 
@app.get("/tasks/{task_id}")
def read_task(task_id: int):
    task = get_task_by_id(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task
 
 
@app.post("/tasks")
def add_tasks(task:Task):
    create_task(task)
    return "tasks created"
 
@app.put("/tasks/{task_id}")
def update(task_id:int,task:Task):
    update_task(task_id,task)
    return "task updated"
 
@app.delete("/tasks/{task_id}")
def delete_task(task_id:int):
    delete(task_id)
    return "task deleted"