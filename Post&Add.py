from typing import List, Dict, Any, Union
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel, create_model
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

app = FastAPI(title="MASTERLIST")

client = AsyncIOMotorClient("mongodb://localhost:27017/")
db = client["databasename"]
collection = db["masterlist"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],  
    allow_headers=["*"],  
)

class FieldModel(BaseModel):
    col_name: str
    type: Union[int, str, bool, float]
    unique: str

class SchemaModel(BaseModel):
    schema_name: str
    fields: List[FieldModel]

async def get_schemas() -> List[SchemaModel]:
    schemas = []
    async for document in collection.find({}):
        schema = SchemaModel(**document)
        schemas.append(schema)
    return schemas

async def setup_routes():
    schemas = await get_schemas()
    for schema in schemas:
        generate_routes_from_schema(schema)

def generate_routes_from_schema(schema: SchemaModel):
    schema_name = schema.schema_name
    fields = {field.col_name: field.type for field in schema.fields}

    CustomModel = create_model(schema_name, **fields)

    @app.post(f"/{schema_name}/")
    async def add_item(item: CustomModel = Body(...)) -> Dict[str, Any]:
     item_data = item.dict()
     item_data["created_at"] = datetime.now().strftime("%d/%m/%Y")

     
     schema_definition = await collection.find_one({"schema_name": schema_name})
     if schema_definition:
          for field in schema.fields:
               if field.unique.upper() == "Y": 
                    existing_item = await db[schema_name].find_one({field.col_name: item_data[field.col_name]})
                    if existing_item:
                         raise HTTPException(status_code=400, detail=f"{field.col_name} must be unique")

     
     await db[schema_name].insert_one(item_data)
     return {"message": "Schema added successfully with creation date"}

app.add_event_handler("startup", setup_routes)

@app.post("/add-schema/")
async def add_schema(schema: SchemaModel = Body(...)) -> Dict[str, Any]:
    
    current_date = datetime.now().strftime("%d/%m/%y")
    schema_dict = schema.dict()
    schema_dict["created_at"] = current_date
    
    schema_name = schema_dict["schema_name"]
    existing_schema = await collection.find_one({"schema_name": schema_name})
    if existing_schema:
        raise HTTPException(status_code=400, detail="Schema with the same name already exists")

    await collection.insert_one(schema_dict)
    
    schema_fields = {field.col_name: 1 for field in schema.fields if field.col_name != "created_at"}  
    schema_collection = db[schema_name]
    await schema_collection.create_index(list(schema_fields.items()), unique=True)
    
    return {"message": "Schema added successfully"}



