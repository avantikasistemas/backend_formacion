from Utils.tools import Tools, CustomException
from sqlalchemy import text
from Models.tipo_nivel_formacion_model import TipoNivelFormacionModel
from Models.tipo_actividad_model import TipoActividadModel
from Models.ciudades_formacion_model import CiudadesFormacionModel
from Models.tipos_competencia_formacion_model import TiposCompetenciaFormacionModel
from Models.macroprocesos_model import MacroprocesosModel
from Models.tipo_modalidad_model import TipoModalidadModel

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
            raise CustomException(str(ex))
        finally:
            self.db.close()

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

    def get_macroprocesos(self):

        try:
            query = self.db.query(
                MacroprocesosModel
            ).filter(
                MacroprocesosModel.estado == 1
            ).all()                 

            # Retornar directamente una lista de diccionarios
            return [{"id": key.id, "nombre": key.nombre} for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

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
