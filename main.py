import logging
import sys
from fastapi import FastAPI, UploadFile, File

app = FastAPI()
#post is when a user is providing server with a file 
@app.post("/upload")
#File parameter tells fast api not to expect a json
#"..." tells fastapi not to do anything if not a file type
async def main(file: UploadFile = File(...)):
    stream = file.file
    return "File uploaded successfully"




if __name__ == "__main__":
    main()