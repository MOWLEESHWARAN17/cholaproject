from typing import List, Dict, Any, Union, Optional
from fastapi import FastAPI, HTTPException, Body
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

class FieldModel(BaseModel):
    col_name: str
    type: Union[int, str, bool, float, List, Dict[str, Any]]
    unique: bool
    selected_value: Optional[str] = None
    allowed_values: Optional[List[str]] = None  # Added allowed_values field
    dict_keys: Optional[Dict[str, Any]] = None  # Added dict_keys field

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
    fields = {field.col_name: field for field in schema.fields}

    CustomModel = create_model(schema_name, **{field.col_name: (field.type, ...) for field in schema.fields})

    @app.post(f"/{schema_name}/")
    async def add_item(item: CustomModel = Body(...)) -> Dict[str, Any]:
        item_data = item.dict()
        item_data["created_at"] = datetime.now().strftime("%d/%m/%Y")

        schema_definition = await collection.find_one({"schema_name": schema_name})
        if schema_definition:
            for field_name, field in fields.items():
                if field.unique:
                    existing_item = await db[schema_name].find_one({field_name: item_data[field_name]})
                    if existing_item:
                        raise HTTPException(status_code=400, detail=f"{field_name} must be unique")
                if field.allowed_values is not None:
                    field_value = item_data[field_name]
                    if field_value not in field.allowed_values:
                        raise HTTPException(status_code=400, detail=f"{field_name} must be one of {', '.join(field.allowed_values)}")
                    else:
                        item_data[field_name] = field_value  # Update selected value

                if field.dict_keys is not None:
                    item_data[field_name] = {k: v for k, v in item_data[field_name].items() if k in field.dict_keys}

        await db[schema_name].insert_one(item_data)
        return {"message": "Schema added successfully with creation date"}

    @app.put(f"/{schema_name}/{{item_id}}")
    async def update_schema_item(item_id: str, item: Dict[str, Any]) -> Dict[str, str]:
        try:
            object_id = ObjectId(item_id)
        except Exception as e:
            raise HTTPException(status_code=400, detail="Invalid ObjectId")

        lcollection = db[schema_name]
        schema_definition = await collection.find_one({"schema_name": schema_name})

        if schema_definition:
            field_to_update = None

            # Find the field to update
            for field_name, field in fields.items():
                if field_name in item:
                    field_to_update = field_name
                    break

            if not field_to_update:
                raise HTTPException(status_code=400, detail="No valid field provided for update")

            # Check if the field exists in the schema's collection
            existing_item = await lcollection.find_one({"_id": object_id})
            if existing_item:
                # Check uniqueness if the field is marked as unique
                for field_name, field in fields.items():
                    if field_name == field_to_update and field.unique:
                        existing_item_with_value = await lcollection.find_one({field_to_update: item[field_to_update]})
                        if existing_item_with_value and existing_item_with_value["_id"] != object_id:
                            raise HTTPException(status_code=400, detail=f"{field_to_update} must be unique")
                        break

                # Check allowed values if specified
                if field.allowed_values is not None:
                    field_value = item[field_to_update]
                    if field_value not in field.allowed_values:
                        raise HTTPException(status_code=400, detail=f"{field_to_update} must be one of {', '.join(field.allowed_values)}")
                    else:
                        item[field_to_update] = field_value  # Update selected value

                # Check and update dict keys
                if field.dict_keys is not None:
                    item[field_to_update] = {k: v for k, v in item[field_to_update].items() if k in field.dict_keys}

                # Update the field
                await lcollection.update_one({"_id": object_id}, {"$set": {field_to_update: item[field_to_update]}})
                return {"message": f"Field '{field_to_update}' updated successfully for item with ID '{item_id}'"}
            else:
                return {"message": f"No item found with ID '{item_id}' in collection '{schema_name}'"}
        else:
            return {"message": f"Schema '{schema_name}' not found"}


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

@app.get("/get-schemas/")
async def get_schemas_endpoint():
    schemas = await get_schemas()
    return schemas

@app.put("/update-schema/{schema_name}")
async def update_schema(schema_name: str, fields: List[FieldModel]) -> Dict[str, str]:
    # Check if the schema exists
    existing_schema = await collection.find_one({"schema_name": schema_name})
    if not existing_schema:
        raise HTTPException(status_code=404, detail="Schema not found")

    # Iterate through each field in the request
    for field in fields:
        field_name = field.col_name
        field_type = field.type
        unique = field.unique
        allowed_values = field.allowed_values
        dict_keys = field.dict_keys

        # Check if the field exists in the schema
        field_exists = False
        for existing_field in existing_schema['fields']:
            if existing_field['col_name'] == field_name:
                field_exists = True
                # Check if field properties have changed
                if (existing_field['type'] != field_type or 
                    existing_field['unique'] != unique or 
                    existing_field['allowed_values'] != allowed_values or 
                    existing_field['selected_value'] != field.selected_value or 
                    existing_field.get('dict_keys') != dict_keys):
                    update_data = {
                        "fields.$.type": field_type, 
                        "fields.$.unique": unique, 
                        "fields.$.selected_value": field.selected_value,
                        "fields.$.dict_keys": dict_keys
                    }
                    if allowed_values is not None:
                        update_data["fields.$.allowed_values"] = allowed_values

                    await collection.update_one(
                        {"schema_name": schema_name, "fields.col_name": field_name},
                        {"$set": update_data}
                    )
                break

        # If the field does not exist, add it to the schema
        if not field_exists:
            await collection.update_one(
                {"schema_name": schema_name},
                {"$addToSet": {"fields": {"col_name": field_name, "type": field_type, 
                                           "unique": unique, "allowed_values": allowed_values, 
                                           "selected_value": field.selected_value, "dict_keys": dict_keys}}}
            )

    return {"message": f"Schema '{schema_name}' updated successfully"}
