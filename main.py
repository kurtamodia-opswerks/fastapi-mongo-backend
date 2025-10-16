from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from routers import chart, dataset, dashboard, schema_less

app = FastAPI(title="Dataset API", version="1.0")

# CORS settings
origins = [
    "http://localhost:3000",  # Next dev server
    "http://localhost:5173",  
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,         
    allow_credentials=True,
    allow_methods=["*"],           
    allow_headers=["*"],          
)

# include routers
app.include_router(dataset.router, prefix="/api")
app.include_router(chart.router, prefix="/api")
app.include_router(dashboard.router, prefix="/api")
app.include_router(schema_less.router, prefix="/api")

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    print(exc.errors())
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "body": exc.body},
    )

