from fastapi import APIRouter, Request, Depends
from sqlalchemy.orm import Session
from Class.Parametros import Parametros
from Utils.decorator import http_decorator
from Config.db import get_db

parametros_router = APIRouter()

@parametros_router.post('/get_parametros', tags=["Parametros"], response_model=dict)
@http_decorator
def get_parametros(request: Request, db: Session = Depends(get_db)):
    response = Parametros(db).get_parametros()
    return response

@parametros_router.post('/get_proveedores', tags=["Parametros"], response_model=dict)
@http_decorator
def get_proveedores(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Parametros(db).get_proveedores(data)
    return response
