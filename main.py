from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import dataset

app = FastAPI(title="Dataset API", version="1.0")

# CORS settings
origins = [
    "http://localhost:3000",  # React dev server
    "http://localhost:5173",  # Vite dev server
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
    # Add your production frontend URL here later
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,         # Allowed origins
    allow_credentials=True,
    allow_methods=["*"],           # e.g. GET, POST, PUT, DELETE
    allow_headers=["*"],           # Allow all headers
)

# include routers
app.include_router(dataset.router, prefix="/dataset", tags=["Dataset"])
