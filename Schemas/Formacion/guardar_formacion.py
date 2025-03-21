from pydantic import BaseModel

class GuardarFormacion(BaseModel):
    nivel_formacion: int
    tipo_actividad: int
    tema: str
    origen: str
    objetivo_general: str
    objetivo_especifico: str
    modalidad: int
    duracion_horas: int
    duracion_minutos: int
    metodologia: str
    tipo: int
    proveedor: int
    evaluacion: str
    seguimiento: str
    fecha_inicio: str
    fecha_fin: str
