from Utils.tools import Tools, CustomException
from sqlalchemy import text, or_, case
from Models.registro_general_formacion_model import (
    RegistroGeneralFormacionModel as RegistroGeneral
)
from Models.tipo_nivel_formacion_model import TipoNivelFormacionModel
from Models.tipo_actividad_model import TipoActividadModel
from Models.ciudades_formacion_model import CiudadesFormacionModel
from Models.tipos_competencia_formacion_model import TiposCompetenciaFormacionModel
from Models.macroprocesos_model import MacroprocesosModel
from Models.macroprocesos_cargos_model import MacroprocesosCargosModel
from Models.tipo_modalidad_model import TipoModalidadModel
from Models.tipo_estado_formacion_model import TipoEstadoFormacionModel

class Querys:

    def __init__(self, db):
        self.db = db
        self.tools = Tools()

    # Query para obtener la informacion del usuario
    def get_usuario(self, usuario, password):

        try:
            response = dict()
            sql = """
                SELECT des_usuario, nit
                FROM dbo.usuarios
                WHERE usuario = :usuario
                AND clave = :clave;
            """

            query = self.db.execute(
                text(sql), 
                {"usuario": usuario, "clave": password}
            ).fetchone()
            if not query:
                raise CustomException("Usuario o contraseña incorrecta.")
            
            response.update({
                "nombre": query[0],
                "cedula": str(query[1])
            })

            return response
                
        except Exception as ex:
            print(str(ex))
            raise CustomException("Error al intentar conectar con la base de datos.")
        finally:
            self.db.close()

    # Query para obtener los datos de usuario por cedula, esta query solo es
    # usada para validacion del jwt bearer
    def get_usuario_x_cedula(self, cedula):

        try:
            sql = """
                SELECT nit
                FROM dbo.usuarios
                WHERE nit = :cedula;
            """

            query = self.db.execute(text(sql), {"cedula": cedula}).fetchone()

            return query[0]
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para obtener los niveles de formación
    def get_nivel_formacion(self):

        try:
            query = self.db.query(
                TipoNivelFormacionModel
            ).filter(
                TipoNivelFormacionModel.estado == 1
            ).all()                 

            # Retornar directamente una lista de diccionarios
            return [{"id": key.id, "nombre": key.nombre} for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para obtener los tipos de actividad
    def get_tipo_actividad(self):

        try:
            query = self.db.query(
                TipoActividadModel
            ).filter(
                TipoActividadModel.estado == 1
            ).all()                 

            # Retornar directamente una lista de diccionarios
            return [{"id": key.id, "nombre": key.nombre} for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para obtener las ciudades
    def get_ciudades_formacion(self):

        try:
            query = self.db.query(
                CiudadesFormacionModel
            ).filter(
                CiudadesFormacionModel.estado == 1
            ).all()                 

            # Retornar directamente una lista de diccionarios
            return [{"id": key.id, "nombre": key.nombre} for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para obtener los tipos de competencia
    def tipos_competencia_formacion(self, tipo: int):

        try:
            query = self.db.query(
                TiposCompetenciaFormacionModel
            ).filter(
                TiposCompetenciaFormacionModel.tipo == tipo,
                TiposCompetenciaFormacionModel.estado == 1
            ).all()                 

            # Retornar directamente una lista de diccionarios
            return [{"id": key.id, "nombre": key.nombre, "orden": key.orden} for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para obtener los macroprocesos
    def get_macroprocesos(self):

        try:
            query = self.db.query(
                MacroprocesosModel
            ).filter(
                MacroprocesosModel.estado == 1
            ).order_by(
                MacroprocesosModel.nombre
            ).all()

            # Retornar directamente una lista de diccionarios
            return [{"id": key.id, "nombre": key.nombre} for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para obtener las modalidades
    def get_modalidad(self):

        try:
            query = self.db.query(
                TipoModalidadModel
            ).filter(
                TipoModalidadModel.estado == 1
            ).all()                 

            # Retornar directamente una lista de diccionarios
            return [{"id": key.id, "nombre": key.nombre} for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para obtener los proveedores
    def get_proveedores(self, valor: str):

        try:
            sql = """
                SELECT * 
                FROM terceros 
                WHERE concepto_1 IN (1, 3) 
                AND (nit LIKE :valor OR nombres LIKE :valor)
            """
            query = self.db.execute(text(sql), {"valor": f"%{valor}%"}).fetchall()

            # Retornar directamente una lista de diccionarios
            return [{"id": key.id, "nit": key.nit, "nombres": key.nombres} for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para buscar el numero siguiente del consecutivo
    def buscar_numero_siguiente(self):

        try:
            sql = """
                SELECT * 
                FROM consecutivos 
                WHERE tipo = 'FCH';
            """
            query = self.db.execute(text(sql)).fetchone()

            if query:
                return query.siguiente
                
        except Exception as ex:
            print(str(ex))
            raise CustomException("Error al consultar consecutivo.")
        finally:
            self.db.close()

    # Query para insertar datos de la formación.
    def guardar_formacion(self, data: dict):
        try:
            print(data)
            reg = RegistroGeneral(data)
            self.db.add(reg)
            self.db.commit()
        except Exception as ex:
            raise CustomException(str(ex))
        finally:
            self.db.close()
        return True

    # Query para actualizar el siguiente consecutivo
    def actualizar_consecutivo(self, num_siguiente: int):
        try:
            sql = """
                UPDATE consecutivos
                SET siguiente = :siguiente
                WHERE tipo = 'FCH';
            """
            self.db.execute(text(sql), {"siguiente": num_siguiente})
            self.db.commit()
                
        except Exception as ex:
            print("Error al actualizar:", ex)
            self.db.rollback()
            raise CustomException("Error al actualizar.")
        finally:
            self.db.close()

    # Query para obtener las formaciones
    def get_formaciones(self, valor: str):

        try:
            response = list()
            query = self.db.query(
                RegistroGeneral.id,
                RegistroGeneral.codigo,
                RegistroGeneral.tema,
                TipoModalidadModel.nombre.label('modalidad'),
                TipoEstadoFormacionModel.nombre.label('estado_formacion'),
                RegistroGeneral.fecha_inicio,
                RegistroGeneral.fecha_fin,
            ).join(
                TipoModalidadModel,
                TipoModalidadModel.id == RegistroGeneral.modalidad
            ).join(
                TipoEstadoFormacionModel,
                TipoEstadoFormacionModel.id == RegistroGeneral.estado_formacion
            )

            # Aplicar filtro solo si hay un término de búsqueda
            if valor:
                query = query.filter(or_(
                    RegistroGeneral.codigo.like(f"%{valor}%"),
                    RegistroGeneral.tema.like(f"%{valor}%")
                ))

            if query:
                for key in query:
                    response.append({
                        "id": key.id,
                        "codigo": key.codigo,
                        "tema": key.tema,
                        "modalidad": key.modalidad,
                        "estado_formacion": key.estado_formacion,
                        "fecha_inicio": str(key.fecha_inicio),
                        "fecha_fin": str(key.fecha_fin),
                    })

            return response
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para obtener las formacion por id
    def get_formacion_by_id(self, formacion_id: int):

        try:
            query = self.db.query(
                RegistroGeneral.id,
                RegistroGeneral.codigo,
                RegistroGeneral.nivel_formacion,
                TipoNivelFormacionModel.nombre.label('nivel_formacion_nombre'),
                RegistroGeneral.tipo_actividad,
                TipoActividadModel.nombre.label('tipo_actividad_nombre'),
                RegistroGeneral.tema,
                RegistroGeneral.origen,
                RegistroGeneral.objetivo_general,
                RegistroGeneral.objetivo_especifico,
                RegistroGeneral.objetivo_especifico,
                RegistroGeneral.modalidad,
                TipoModalidadModel.nombre.label('modalidad_nombre'),
                RegistroGeneral.duracion_horas,
                RegistroGeneral.duracion_minutos,
                RegistroGeneral.metodologia,
                RegistroGeneral.tipo,
                case(
                    (RegistroGeneral.tipo == 1, "INTERNO"),
                    (RegistroGeneral.tipo == 2, "EXTERNO"),
                    else_="DESCONOCIDO"  # O cualquier otro valor por defecto
                ).label("tipo_nombre"),
                RegistroGeneral.proveedor,
                RegistroGeneral.ciudad,
                CiudadesFormacionModel.nombre.label('ciudad_nombre'),
                RegistroGeneral.evaluacion,
                RegistroGeneral.seguimiento,
                RegistroGeneral.estado_formacion,
                TipoEstadoFormacionModel.nombre.label('estado_formacion_nombre'),
                RegistroGeneral.fecha_inicio,
                RegistroGeneral.fecha_fin,
                RegistroGeneral.created_at,
            ).join(
                TipoNivelFormacionModel,
                TipoNivelFormacionModel.id == RegistroGeneral.nivel_formacion
            ).join(
                TipoActividadModel,
                TipoActividadModel.id == RegistroGeneral.tipo_actividad
            ).join(
                TipoModalidadModel,
                TipoModalidadModel.id == RegistroGeneral.modalidad
            ).join(
                CiudadesFormacionModel,
                CiudadesFormacionModel.id == RegistroGeneral.ciudad
            ).join(
                TipoEstadoFormacionModel,
                TipoEstadoFormacionModel.id == RegistroGeneral.estado_formacion
            ).filter(
                RegistroGeneral.id == formacion_id,
                RegistroGeneral.estado == 1,
                TipoNivelFormacionModel.estado == 1,
                TipoActividadModel.estado == 1,
                TipoModalidadModel.estado == 1,
                CiudadesFormacionModel.estado == 1,
                TipoEstadoFormacionModel.estado == 1,
            ).first()

            # ✅ Convertir directamente en un diccionario
            response = query._asdict() if query else {}
            if response:
                try:
                    sql = """
                        SELECT nombres 
                        FROM terceros 
                        WHERE concepto_1 in (1,3)
                        AND id = :id
                    """
                    consulta = self.db.execute(text(sql), {"id": response["proveedor"]}).fetchone()

                    response["proveedor_nombre"] = consulta[0] if consulta else ''
                        
                except Exception as ex:
                    print(str(ex))
                    raise CustomException(str(ex))
                finally:
                    self.db.close()

            return response
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()
    
    # Query para obtener las formacion por id
    def get_cargos_por_macroproceso(self, macroproceso: list):

        try:
            query = self.db.query(
                MacroprocesosModel.id,
                MacroprocesosModel.nombre,
                MacroprocesosCargosModel.id.label('cargo_id'),
                MacroprocesosCargosModel.nombre.label('cargo_nombre'),
            ).join(
                MacroprocesosCargosModel,
                MacroprocesosCargosModel.macroproceso_id == MacroprocesosModel.id
            ).filter(
                MacroprocesosModel.estado == 1,
                MacroprocesosCargosModel.estado == 1,
                MacroprocesosCargosModel.macroproceso_id.in_(macroproceso)
            ).all()

            # Retornar directamente una lista de diccionarios
            return [{"id": key.id, "macro_nombre": key.nombre, "cargo_id": key.cargo_id, "cargo_nombre": key.cargo_nombre} for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()
