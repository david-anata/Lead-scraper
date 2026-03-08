from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def home():
    return {"status": "Agent server running"}

@app.post("/run-icp-build")
def run_icp_build():
    return {"message": "ICP build endpoint working"}
