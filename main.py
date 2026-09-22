"""应用入口：保留根路径行为，并挂载协调能力路由。"""

from fastapi import FastAPI

from coordination.api import create_app

app = create_app()


@app.get("/")
def read_root():
    return {"Hello": "World"}
