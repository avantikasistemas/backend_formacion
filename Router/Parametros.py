from fastapi import APIRouter, Request, Depends
from sqlalchemy.orm import Session
from Class.Parametros import Parametros
from Utils.decorator import http_decorator
from Config.db import get_db
from Middleware.jwt_bearer import JWTBearer

parametros_router = APIRouter()

@parametros_router.post('/get_parametros', tags=["Parametros"], response_model=dict, dependencies=[Depends(JWTBearer())])
@http_decorator
def get_parametros(request: Request, db: Session = Depends(get_db)):
    response = Parametros(db).get_parametros()
    return response

@parametros_router.post('/get_proveedores', tags=["Parametros"], response_model=dict, dependencies=[Depends(JWTBearer())])
@http_decorator
def get_proveedores(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Parametros(db).get_proveedores(data)
    return response

@parametros_router.post('/get_cargos_por_macroproceso', tags=["Parametros"], response_model=dict, dependencies=[Depends(JWTBearer())])
@http_decorator
def get_cargos_por_macroproceso(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Parametros(db).get_cargos_por_macroproceso(data)
    return response

@parametros_router.post('/get_formacion_estados', tags=["Parametros"], response_model=dict, dependencies=[Depends(JWTBearer())])
@http_decorator
def get_formacion_estados(request: Request, db: Session = Depends(get_db)):
    response = Parametros(db).get_formacion_estados()
    return response
