from fastapi import APIRouter, Request, Depends
from sqlalchemy.orm import Session
from Schemas.Formacion.guardar_formacion import GuardarFormacion
from Class.Formacion import Formacion
from Utils.decorator import http_decorator
from Config.db import get_db
from Middleware.jwt_bearer import JWTBearer

formacion_router = APIRouter()

@formacion_router.post('/guardar_formacion', tags=["Formacion"], response_model=dict, dependencies=[Depends(JWTBearer())])
@http_decorator
def guardar_formacion(request: Request, formacion: GuardarFormacion, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Formacion(db).guardar_formacion(data)
    return response

@formacion_router.post('/get_formaciones', tags=["Formacion"], response_model=dict, dependencies=[Depends(JWTBearer())])
@http_decorator
def get_formaciones(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Formacion(db).get_formaciones(data)
    return response

@formacion_router.post('/get_formacion_by_id', tags=["Formacion"], response_model=dict, dependencies=[Depends(JWTBearer())])
@http_decorator
def get_formacion_by_id(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Formacion(db).get_formacion_by_id(data)
    return response
