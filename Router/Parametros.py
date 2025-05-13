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

@parametros_router.post('/get_personal_interno', tags=["Parametros"], response_model=dict, dependencies=[Depends(JWTBearer())])
@http_decorator
def get_personal_interno(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Parametros(db).get_personal_interno(data)
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
    data = getattr(request.state, "json_data", {})
    response = Parametros(db).get_formacion_estados(data)
    return response

@parametros_router.post('/obtener_todo_personal_activo', tags=["Parametros"], response_model=dict, dependencies=[Depends(JWTBearer())])
@http_decorator
def obtener_todo_personal_activo(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Parametros(db).obtener_todo_personal_activo(data)
    return response
