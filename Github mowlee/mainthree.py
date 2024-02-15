from typing import List, Dict, Any, Union
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel, create_model, ConstrainedStr
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
from bson import ObjectId

# Initialize FastAPI app
app = FastAPI(title="MASTERLIST")

# Connect to MongoDB
client = AsyncIOMotorClient("mongodb://localhost:27017/")
db = client["databasename"]
collection = db["masterlist"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow requests from all origins (replace with your frontend URL in production)
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],  # Add allowed HTTP methods
    allow_headers=["*"],  # Allow all headers
)

class UniqueFlag(ConstrainedStr):
    regex = r"^[YN]$"

class FieldModel(BaseModel):
    col_name: str
    type: Union[int, str, bool, float]
    unique: UniqueFlag

class SchemaModel(BaseModel):
    schema_name: str
    fields: List[FieldModel]
    created_at: str

async def get_schemas() -> List[SchemaModel]:
    schemas = []
    async for document in collection.find({}):
        schema = SchemaModel(**document)
        schemas.append(schema)
    return schemas

async def create_schema(schema: SchemaModel) -> None:
    await collection.insert_one(schema.dict())

async def get_schema_by_name(schema_name: str) -> SchemaModel:
    document = await collection.find_one({"schema_name": schema_name})
    if document:
        return SchemaModel(**document)
    else:
        raise HTTPException(status_code=404, detail="Schema not found")

async def setup_routes():
    schemas = await get_schemas()
    for schema in schemas:
        generate_routes_from_schema(schema)

def generate_routes_from_schema(schema: SchemaModel):
    schema_name = schema.schema_name
    fields = {field.col_name: field.type for field in schema.fields}

    CustomModel = create_model(schema_name, **fields)

    @app.post(f"/{schema_name}/", tags=[schema_name])
    async def add_item(item: CustomModel = Body(...)) -> Dict[str, Any]:
        item_data = item.dict()
        item_data["created_at"] = datetime.now().strftime("%d/%m/%Y")

        # Check uniqueness constraints
        schema_definition = await collection.find_one({"schema_name": schema_name})
        if schema_definition:
            for field in schema.fields:
                if field.unique.upper() == "Y":
                    existing_item = await db[schema_name].find_one({field.col_name: item_data[field.col_name]})
                    if existing_item:
                        raise HTTPException(status_code=400, detail=f"{field.col_name} must be unique")

        # Insert data into the collection named after schema_name
        await db[schema_name].insert_one(item_data)
        return {"message": "Item added successfully"}

    @app.get(f"/{schema_name}/", response_model=List[CustomModel], tags=[schema_name])
    async def get_items() -> List[Dict[str, Any]]:
        """
        Retrieve all items for a specific schema.
        """
        items = []
        async for document in db[schema_name].find({}):
            items.append(CustomModel(**document))
        return items

    @app.get(f"/{schema_name}/{{item_id}}", response_model=CustomModel, tags=[schema_name])
    async def get_item(item_id: str) -> Dict[str, Any]:
        """
        Retrieve a single item by its ID for a specific schema.
        """
        obj_id = ObjectId(item_id)
        item = await db[schema_name].find_one({"_id": obj_id})
        if item:
            return CustomModel(**item)
        else:
            raise HTTPException(status_code=404, detail="Item not found")

    @app.put(f"/{schema_name}/{{item_id}}", tags=[schema_name])
    async def update_item(item_id: str, item: CustomModel = Body(...)) -> Dict[str, Any]:
        """
        Update an item by its ID for a specific schema.
        """
        obj_id = ObjectId(item_id)
        item_data = item.dict()
        item_data["created_at"] = datetime.now().strftime("%d/%m/%Y")
        result = await db[schema_name].update_one({"_id": obj_id}, {"$set": item_data})
        if result.modified_count == 1:
            return {"message": "Item updated successfully"}
        else:
            raise HTTPException(status_code=404, detail="Item not found")

    @app.delete(f"/{schema_name}/{{item_id}}", tags=[schema_name])
    async def delete_item(item_id: str) -> Dict[str, Any]:
        """
        Delete an item by its ID for a specific schema.
        """
        obj_id = ObjectId(item_id)
        result = await db[schema_name].delete_one({"_id": obj_id})
        if result.deleted_count == 1:
            return {"message": "Item deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Item not found")

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

@app.get("/")
async def root():
    return {"message": "Welcome to the MASTERLIST API!"}

@app.get("/get-schema/{schema_name}", response_model=SchemaModel)
async def get_schema(schema_name: str) -> Dict[str, Any]:
    """
    Retrieve a schema by its name.
    """
    schema = await get_schema_by_name(schema_name)
    schema_dict = schema.dict()
    schema_dict["created_at"] = datetime.strptime(schema_dict["created_at"], "%d/%m/%y").strftime("%Y-%m-%d %H:%M:%S")
    return schema_dict

@app.get("/get-schemas-with-date", response_model=List[Dict[str, str]])
async def get_schemas_with_date():
    """
    Retrieve all schemas with their creation dates.
    """
    schemas = await get_schemas()
    schemas_with_date = [{"schema_name": schema.schema_name, "created_at": schema.created_at} for schema in schemas]
    return schemas_with_date


app.add_event_handler("startup", setup_routes)
