from typing import List, Dict, Any, Union, Optional, Type
from fastapi import FastAPI, HTTPException, Body, Query
from pydantic import BaseModel, create_model
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
from bson import ObjectId

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


#--------------Basemodels--------------#

class FieldModel(BaseModel):
    col_name: str
    type: Union[str, Type[int], Type[str], Type[bool], Type[float], Type[List], Type[Dict[str, Any]]]
    unique: Optional[bool]
    selected_value: Optional[str] = None
    allowed_values: Optional[List[str]] = None
    dict_keys: Optional[Dict[str, Any]] = None


class SchemaModel(BaseModel):
    schema_name: str
    fields: List[FieldModel]

#--------------Adding a New Schema--------------#

@app.post("/add-schema/",tags=["Common Routes"])
async def add_schema(schema: SchemaModel = Body(...)) -> Dict[str, Any]:
    current_date = datetime.now().strftime("%d/%m/%Y")
    schema_dict = schema.dict()
    schema_dict["created_at"] = current_date
    schema_name = schema_dict["schema_name"]
    existing_schema = await collection.find_one({"schema_name": schema_name})
    if existing_schema:
        raise HTTPException(status_code=400, detail="Schema with the same name already exists")
    fields = []
    for field in schema.fields:
        field_info = {"col_name": field.col_name}
        if field.type == "list":
            field_info["type"] = "list"
            field_info["allowed_values"] = field.allowed_values if field.allowed_values else []
        elif field.type == "dict":
            field_info["type"] = "dict"
            field_info["dict_keys"] = field.dict_keys if field.dict_keys else {}
        else:
            field_info["type"] = field.type
            field_info["unique"] = field.unique
        fields.append(field_info)
    schema_dict["fields"] = fields
    await collection.insert_one(schema_dict)
    return {"message": "Schema added successfully"}

#--------------Replacing fields in schema--------------#

@app.put("/replacefields/{schema_name}",tags=["Common Routes"])
async def replace_schema_fields(schema_name: str, new_fields: List[Dict[str, Any]]) -> Dict[str, str]:
    existing_schema = await collection.find_one({"schema_name": schema_name})
    if not existing_schema:
        raise HTTPException(status_code=404, detail="Schema not found")
    new_schema_data = {
        "schema_name": schema_name,
        "created_at": datetime.now().strftime("%d/%m/%Y"),
        "fields": new_fields
    }
    for field in new_schema_data["fields"]:
        if field.get("unique") == "true":
            field["unique"] = True
        elif field.get("unique") == "false":
            field["unique"] = False
    await collection.replace_one(
        {"schema_name": schema_name},
        new_schema_data
    )
    return {"message": f"Schema '{schema_name}' fields replaced successfully"}


#--------------Generate routing for adding datas inside schema--------------#

async def get_schemas() -> List[SchemaModel]:
    schemas = []
    async for document in collection.find({}):
        schema = SchemaModel(**document)
        schemas.append(schema)
    return schemas

async def setup_routes():
    schemas = await get_schemas()
    for schema in schemas:
        await generate_routes_from_schema(schema)

app.add_event_handler("startup", setup_routes)


async def find_existing_item(schema_name: str, col_name: str, value: Any) -> Optional[Dict[str, Any]]:
    item = await collection.find_one({col_name: value})
    return item

async def generate_routes_from_schema(schema: SchemaModel):
    schema_name = schema.schema_name
    fields = {field.col_name: field for field in schema.fields}

    CustomModel = create_model(schema_name, **{field.col_name: (field.type, ...) for field in fields.values()})

    @app.get(f"/{schema_name}/", response_model=List[CustomModel], tags=[schema_name])
    async def get_items(page: int = Query(1, gt=0), page_size: int = Query(10, gt=0)) -> List[CustomModel]:
        skip = (page - 1) * page_size
        items_cursor = db[schema_name].find({}).skip(skip).limit(page_size)
        items = await items_cursor.to_list(length=None)
        return items


    # Get item by ID for the specified schema
    @app.get(f"/{schema_name}/{{item_id}}", response_model=CustomModel, tags=[schema_name])
    async def get_item_by_id(item_id: str) -> CustomModel:
        item = await db[schema_name].find_one({"_id": ObjectId(item_id)})
        if item:
            return item
        else:
            raise HTTPException(status_code=404, detail=f"Item not found for ID: {item_id}")

    # Get all items for the specified schema
    @app.post(f"/{schema_name}/fields/", response_model=List[Dict[str, Any]], tags=[schema_name])
    async def get_items_by_fields(query_string: str) -> List[Dict[str, Any]]:
        """
        Retrieve items for a specific schema based on provided query parameters.
        """
        print("Query String:", query_string)
    
        # Parse query parameters
        query_pairs = query_string.split(",")
        print("Query Pairs:", query_pairs)
    
        # Construct query dictionary
        query = {}
        for pair in query_pairs:
            field, value = pair.split(":")
            if field in fields:
                query[field] = value
    
        print("Constructed Query:", query)
    
        # Fetch documents from the database matching the query
        items = []
        async for document in db[schema_name].find(query):
            # Convert ObjectId to string
            document["_id"] = str(document["_id"])
            items.append(document)
    
        return items

    # Adding an item inside any schema
    @app.post(f"/{schema_name}/", tags=[schema_name])
    async def add_item(item_data: CustomModel = Body(...)) -> Dict[str, Any]:
     async def fetch_schema_definition(schema_name: str) -> SchemaModel:
          schema_definition = await collection.find_one({"schema_name": schema_name})
          if schema_definition:
               return SchemaModel(**schema_definition)
          else:
               return None

     schema_definition = await fetch_schema_definition(schema_name)
     if not schema_definition:
          raise HTTPException(status_code=404, detail="Schema not found")

     # Validate uniqueness constraints for fields with unique=True
     for field in schema_definition.fields:
          if field.unique:
               # Check if the value already exists in the collection for fields with unique constraint
               existing_item = await db[schema_name].find_one({field.col_name: item_data.dict().get(field.col_name)})
               if existing_item:
                    raise HTTPException(status_code=400, detail=f"{field.col_name} must be unique")

     # Validate list fields against allowed values
     for field in schema_definition.fields:
          if field.type == "list" and field.allowed_values:
               field_value = item_data.dict().get(field.col_name)
               if not all(value in field.allowed_values for value in field_value):
                    raise HTTPException(status_code=400, detail=f"Invalid value for {field.col_name}")

     # Validate dict field keys against specified dict_keys
     for field in schema_definition.fields:
          if field.type == "dict" and field.dict_keys:
               field_value = item_data.dict().get(field.col_name, {})
               for key in field_value.keys():
                    if key not in field.dict_keys:
                         raise HTTPException(status_code=400, detail=f"Invalid key for {field.col_name}: {key}")

     # Insert the item data into the collection
     await db[schema_name].insert_one(item_data.dict())
     return {"message": "Item added successfully"}

    @app.put(f"/update/{schema_name}/{{item_id}}", tags=[schema_name])
    async def update_schema_item(item_id: str, updated_fields: Dict[str, Any]) -> Dict[str, str]:
          try:
               object_id = ObjectId(item_id)
          except Exception as e:
               raise HTTPException(status_code=400, detail="Invalid ObjectId")
          
          lcollection = db[schema_name]
          schema_definition = await collection.find_one({"schema_name": schema_name})
          
          if schema_definition:
               schema_model = SchemaModel(**schema_definition)
               for field_name, updated_value in updated_fields.items():
                    # Check if the field exists in the schema's fields
                    field_exists = any(field.col_name == field_name for field in schema_model.fields)
                    if not field_exists:
                         raise HTTPException(status_code=400, detail=f"Field '{field_name}' not found in schema '{schema_name}'")

                    # Find the field model
                    field_model = next(field for field in schema_model.fields if field.col_name == field_name)

                    # Check if the field is marked as unique
                    if field_model.unique:
                         # Check if the new value already exists in the collection
                         existing_item_with_value = await lcollection.find_one({field_name: updated_value})
                         if existing_item_with_value and existing_item_with_value["_id"] != object_id:
                              raise HTTPException(status_code=400, detail=f"{field_name} must be unique")
                    
                    # Check if the field has allowed values
                    if field_model.allowed_values:
                         if updated_value not in field_model.allowed_values:
                              raise HTTPException(status_code=400, detail=f"Invalid value for {field_name}")

                    # Update the field
                    await lcollection.update_one({"_id": object_id}, {"$set": {field_name: updated_value}})
               
               return {"message": f"Fields updated successfully for item with ID '{item_id}' in collection '{schema_name}'"}
          else:
               return {"message": f"Schema '{schema_name}' not found"}



@app.get("/getfields/{schema_name}/",tags=["Common Routes"])
async def get_schema_field(schema_name: str) -> Dict[str, Any]:
    schema_data = await collection.find_one({"schema_name": schema_name})
    if not schema_data:
        raise HTTPException(status_code=404, detail="Schema not found")
    
    # Convert ObjectId to string
    schema_data["_id"] = str(schema_data["_id"])

    return schema_data
