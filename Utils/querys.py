from Utils.tools import Tools, CustomException
from sqlalchemy import text, or_, case
from sqlalchemy.sql import select
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
from Models.tipos_competencia_formacion_detalles_model import TiposCompetenciaFormacionDetalleModel
from Models.macroprocesos_formacion_detalles_model import MacroprocesosFormacionDetalleModel
from Models.cargos_formacion_detalles_model import CargosFormacionDetalleModel
from Models.ciudades_formacion_detalles_model import CiudadesFormacionDetalleModel
from Models.personal_formacion_detalle_model import PersonalFormacionDetalleModel
from Models.tipo_origen_necesidad_model import TipoOrigenNecesidadModel
from Models.tipo_evaluacion_model import TipoEvaluacionModel
from Models.calificaciones_formacion_model import CalificacionesFormacionModel
from collections import defaultdict
import json

class Querys:

    def __init__(self, db):
        self.db = db
        self.tools = Tools()
        self.query_params = dict()

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
            reg = RegistroGeneral(data)
            self.db.add(reg)
            self.db.commit()
            reg_id = reg.id
        except Exception as ex:
            raise CustomException(str(ex))
        finally:
            self.db.close()
        return reg_id

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
                RegistroGeneral.created_at,
            ).join(
                TipoModalidadModel,
                TipoModalidadModel.id == RegistroGeneral.modalidad
            ).join(
                TipoEstadoFormacionModel,
                TipoEstadoFormacionModel.id == RegistroGeneral.estado_formacion
            ).filter(
                RegistroGeneral.estado == 1
            )

            # Aplicar filtro solo si hay un término de búsqueda
            if valor:
                query = query.filter(or_(
                    RegistroGeneral.codigo.like(f"%{valor}%"),
                    RegistroGeneral.tema.like(f"%{valor}%"),
                    TipoEstadoFormacionModel.nombre.like(f"%{valor}%"),
                ))

            if query:
                for key in query:
                    response.append({
                        "id": key.id,
                        "codigo": key.codigo,
                        "tema": key.tema.upper(),
                        "modalidad": key.modalidad,
                        "estado_formacion": key.estado_formacion,
                        "fecha_inicio": str(key.fecha_inicio),
                        "fecha_fin": str(key.fecha_fin),
                        "fecha_creacion": self.tools.format_date(str(key.created_at), "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"),
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
                TipoEstadoFormacionModel,
                TipoEstadoFormacionModel.id == RegistroGeneral.estado_formacion
            ).filter(
                RegistroGeneral.id == formacion_id,
                RegistroGeneral.estado == 1,
                TipoNivelFormacionModel.estado == 1,
                TipoActividadModel.estado == 1,
                TipoModalidadModel.estado == 1,
                TipoEstadoFormacionModel.estado == 1,
            ).first()

            # ✅ Convertir directamente en un diccionario
            response = query._asdict() if query else {}
            if response:
                try:
                    sql = """
                        SELECT nombres 
                        FROM terceros 
                        WHERE id = :id
                    """
                    consulta = self.db.execute(text(sql), {"id": response["proveedor"]}).fetchone()

                    response["proveedor_nombre"] = consulta[0] if consulta else ''
                    
                    evaluacion_ids = json.loads(response["evaluacion"])
                    evaluaciones = self.get_evaluaciones_by_id(evaluacion_ids)
                    
                    response["evaluacion"] = evaluaciones
                        
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
            response = list()
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

            if query:
                # Diccionario para agrupar por 'id' y 'macro_nombre'
                grouped_data = defaultdict(lambda: {"macro_nombre": "", "cargos": []})

                for item in query:
                    group = grouped_data[item.id]
                    group["macro_nombre"] = item.nombre
                    group["cargos"].append({
                        "cargo_id": item.cargo_id,
                        "cargo_nombre": item.cargo_nombre
                    })

                # Convertir a lista con la estructura deseada
                response = [{"id": k, "macro_nombre": v["macro_nombre"], "cargos": v["cargos"]} for k, v in grouped_data.items()]

            return response
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para insertar datos las competencias de la formación.
    def guardar_competencias(self, data: dict):
        try:
            comp = TiposCompetenciaFormacionDetalleModel(data)
            self.db.add(comp)
            self.db.commit()
            return True
        except Exception as ex:
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para insertar los macroprocesos de la formación.
    def guardar_macroprocesos(self, data: dict):
        try:
            macr = MacroprocesosFormacionDetalleModel(data)
            self.db.add(macr)
            self.db.commit()
            return True
        except Exception as ex:
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para insertar los cargos de la formación.
    def guardar_cargos(self, data: dict):
        try:
            carg = CargosFormacionDetalleModel(data)
            self.db.add(carg)
            self.db.commit()
            return True
        except Exception as ex:
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para insertar las ciudades de la formación.
    def guardar_ciudades(self, data: dict):
        try:
            ciu = CiudadesFormacionDetalleModel(data)
            self.db.add(ciu)
            self.db.commit()
            return True
        except Exception as ex:
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para traer los detalles de las competencias elegidas
    def get_competencias_detalles(self, formacion_id):
        
        try:
            response = dict()            
            
            # Subquery para obtener los ids de tipos de competencia formacion detalle donde formacion_id = 14
            subquery = (
                select(TiposCompetenciaFormacionDetalleModel.tipo_competencia_id)
                .where(
                    TiposCompetenciaFormacionDetalleModel.formacion_id == formacion_id,
                    TiposCompetenciaFormacionDetalleModel.estado == 1
                )
                .scalar_subquery()
            )

            # Consulta 1
            query_competencia_corporativa = (
                self.db.query(TiposCompetenciaFormacionModel)
                .filter(
                    TiposCompetenciaFormacionModel.id.in_(subquery),
                    TiposCompetenciaFormacionModel.tipo == 1,
                    TiposCompetenciaFormacionModel.estado == 1
                )
            ).all()

            # Consulta 2
            query_competencia_rol = (
                self.db.query(TiposCompetenciaFormacionModel)
                .filter(
                    TiposCompetenciaFormacionModel.id.in_(subquery),
                    TiposCompetenciaFormacionModel.tipo == 2,
                    TiposCompetenciaFormacionModel.estado == 1
                )
            ).all()

            # Consulta 3
            query_competencia_posicion = (
                self.db.query(TiposCompetenciaFormacionModel)
                .filter(
                    TiposCompetenciaFormacionModel.id.in_(subquery),
                    TiposCompetenciaFormacionModel.tipo == 3,
                    TiposCompetenciaFormacionModel.estado == 1
                )
            ).all()
            
            response = {
                "competencia_corporativa": [{"id": key.id, "nombre": key.nombre, "orden": key.orden} for key in query_competencia_corporativa] if query_competencia_corporativa else [],
                "competencia_rol": [{"id": key.id, "nombre": key.nombre, "orden": key.orden} for key in query_competencia_rol] if query_competencia_rol else [], 
                "competencia_posicion": [{"id": key.id, "nombre": key.nombre, "orden": key.orden} for key in query_competencia_posicion] if query_competencia_posicion else []
            }
            
            # Retornar directamente una lista de diccionarios
            return response
                            
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para traer los detalles de los macroprocesos y cargos
    def get_macroprocesos_cargos_detalles(self, formacion_id):
        
        try:
            response = dict()
            
            # Subconsulta para obtener los macroproceso_id
            subquery_macroprocesos = (
                select(MacroprocesosFormacionDetalleModel.macroproceso_id)
                .where(
                    MacroprocesosFormacionDetalleModel.estado == 1,
                    MacroprocesosFormacionDetalleModel.formacion_id == formacion_id
                )
                .scalar_subquery()
            )

            # Consulta principal
            query_macroprocesos = (
                self.db.query(MacroprocesosModel)
                .filter(
                    MacroprocesosModel.id.in_(subquery_macroprocesos),
                    MacroprocesosModel.estado == 1
                )
                .order_by(MacroprocesosModel.nombre)
            ).all()

            # Consulta principal
            query_cargos = self.db.query(
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
                MacroprocesosCargosModel.macroproceso_id.in_(subquery_macroprocesos)
            ).all()

            if query_cargos:
                # Diccionario para agrupar por 'id' y 'macro_nombre'
                grouped_data = defaultdict(lambda: {"macro_nombre": "", "cargos": []})

                for item in query_cargos:
                    group = grouped_data[item.id]
                    group["macro_nombre"] = item.nombre
                    group["cargos"].append({
                        "cargo_id": item.cargo_id,
                        "cargo_nombre": item.cargo_nombre
                    })

            response = {
                "macroprocesos": [{"id": key.id, "nombre": key.nombre} for key in query_macroprocesos] if query_macroprocesos else [],
                "cargos": [{"id": k, "macro_nombre": v["macro_nombre"], "cargos": v["cargos"]} for k, v in grouped_data.items()]
            }
            
            # Retornar directamente una lista de diccionarios
            return response
                            
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para traer los detalles de las ciudades elegidas
    def get_ciudades_detalles(self, formacion_id):
        
        try:      
            
            # Subquery para obtener los ids de tipos de ciudades
            subquery = (
                select(CiudadesFormacionDetalleModel.ciudad_id)
                .where(
                    CiudadesFormacionDetalleModel.formacion_id == formacion_id,
                    CiudadesFormacionDetalleModel.estado == 1
                )
                .scalar_subquery()
            )

            query_ciudades = (
                self.db.query(CiudadesFormacionModel)
                .filter(
                    CiudadesFormacionModel.id.in_(subquery),
                    CiudadesFormacionModel.estado == 1
                )
            ).all()

            # Retornar directamente una lista de diccionarios
            return [{"id": key.id, "nombre": key.nombre} for key in query_ciudades] if query_ciudades else []

        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para obtener los estados de la formación
    def get_formacion_estados(self):

        try:
            query = self.db.query(
                TipoEstadoFormacionModel
            ).filter(
                TipoEstadoFormacionModel.estado == 1
            ).all()                 

            # Retornar directamente una lista de diccionarios
            return [{"id": key.id, "nombre": key.nombre} for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para actualizar la formación
    def actualizar_formacion(self, formacion_id: int, data: dict):

        try:
            query = self.db.query(
                RegistroGeneral
            ).filter_by(
                id = formacion_id
            ).update(data)                     
            self.db.commit()
            
            if not query:
                raise CustomException("Formación inexistente.")
            
            return True
                
        except Exception as ex:
            print(ex)
            raise CustomException("Error al actualizar formación.")
        finally:
            self.db.close()

    # Query para obtener el personal activo segun los cargos llenados x formacion.
    def get_personal_activo(self, formacion_id: int):

        try:           
            
            sql = """
                SELECT nit, nombres 
                FROM v_personal_activo 
                WHERE cargo in (
                    SELECT cargo_y_personal 
                    FROM macroprocesos_cargos 
                    WHERE id in (
                        SELECT cargo_id FROM cargos_formacion_detalles WHERE formacion_id = :formacion AND estado = 1
                    ))
                ORDER BY nombres
            """
            query = self.db.execute(text(sql), ({"formacion": formacion_id})).fetchall()

            # Retornar directamente una lista de diccionarios
            return [{"cedula": key[0], "nombre": key[1]} for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException("Error al obtener personal.")
        finally:
            self.db.close()

    # Query para desactivar personal por formacion id
    def desactivar_personal_x_formacion(self, formacion_id: int):
        
        try:
            query = self.db.query(
                PersonalFormacionDetalleModel
            ).filter(
                PersonalFormacionDetalleModel.estado == 1,
                PersonalFormacionDetalleModel.formacion_id == formacion_id
            ).all()
            
            if query:
                for key in query:
                    key.estado = 0
                    self.db.commit()
                        
            return True
                
        except Exception as ex:
            print(str(ex))
            raise CustomException("Error al desactivar personal.")
        finally:
            self.db.close()

    # Query para insertar el personal de la formación.
    def guardar_personal_formacion(self, data: dict):
        try:
            pers = PersonalFormacionDetalleModel(data)
            self.db.add(pers)
            self.db.commit()
            return True
        except Exception as ex:
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para obtener el personal segun la formación id
    def get_personal_formacion(self, formacion_id: int):
        
        try:
            response = list()
            query = self.db.query(
                PersonalFormacionDetalleModel
            ).filter(
                PersonalFormacionDetalleModel.estado == 1,
                PersonalFormacionDetalleModel.formacion_id == formacion_id
            ).all()
            
            if query:
                for key in query:
                    cedula = key.nit
                    response.append(self.get_extra_data_personal(cedula))
                    
                # ✅ Ordenar por nombre
                response.sort(key=lambda x: x['nombre'])

            return response
                
        except Exception as ex:
            print(str(ex))
            raise CustomException("Error al obetener datos del personal.")
        finally:
            self.db.close()

    # Query para traer los datos extra del personal elegido
    def get_extra_data_personal(self, cedula):
        
        try:
            result = dict()
            sql = """
                SELECT nombres, descripcion 
                FROM v_personal_activo 
                WHERE nit = :nit
            """
            consulta = self.db.execute(text(sql), {"nit": cedula}).fetchone()
            if consulta:
                result = {
                    "cedula": cedula,
                    "nombre": consulta[0],
                    "cargo": consulta[1],
                }

            return result
                
        except Exception as ex:
            print(str(ex))
            raise CustomException("Error al obtener datos del personal.")
        finally:
            self.db.close()

    # Query para desactivar tanto macroproceso como cargo por formacion id
    def desactivar_macro_y_cargo_x_id(self, formacion_id: int):
        
        try:
            query = self.db.query(
                MacroprocesosFormacionDetalleModel
            ).filter(
                MacroprocesosFormacionDetalleModel.estado == 1,
                MacroprocesosFormacionDetalleModel.formacion_id == formacion_id
            ).all()
            
            if query:
                for key in query:
                    key.estado = 0
                    self.db.commit()
            self.db.close()
                    
            query_cargos = self.db.query(
                CargosFormacionDetalleModel
            ).filter(
                CargosFormacionDetalleModel.estado == 1,
                CargosFormacionDetalleModel.formacion_id == formacion_id
            ).all()
            
            if query_cargos:
                for key in query_cargos:
                    key.estado = 0
                    self.db.commit()
            self.db.close()
            
            query_personal = self.db.query(
                PersonalFormacionDetalleModel
            ).filter(
                PersonalFormacionDetalleModel.estado == 1,
                PersonalFormacionDetalleModel.formacion_id == formacion_id
            ).all()
            
            if query_personal:
                for key in query_personal:
                    key.estado = 0
                    self.db.commit()
                        
            return True
                
        except Exception as ex:
            print(str(ex))
            raise CustomException("Error al desactivar macroproceso o cargos.")
        finally:
            self.db.close()

    # Query para obtener el estado de la formación
    def obtener_estado_formacion(self, formacion_id: int):
        
        try:
            query = self.db.query(
                RegistroGeneral
            ).filter(
                RegistroGeneral.estado == 1,
                RegistroGeneral.id == formacion_id
            ).first()
            
            if query:     
                return query
                
        except Exception as ex:
            print(str(ex))
            raise CustomException("Error al obtener datos de la formación.")
        finally:
            self.db.close()

    # Query para obtener el personal activo segun los cargos llenados x formacion.
    def obtener_todo_personal_activo(self, valor):

        try:           
            
            sql = """
                SELECT nit, nombres FROM v_personal_activo WHERE nombres LIKE :valor;
            """
            query = self.db.execute(text(sql), {"valor": f"%{valor}%"}).fetchall()

            # Retornar directamente una lista de diccionarios
            return [{"cedula": key[0], "nombre": key[1]} for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException("Error al obtener personal.")
        finally:
            self.db.close()

    # Query para obtener el personal activo.
    def get_personal_interno(self, valor):

        try:           
            
            sql = """
                SELECT vpa.nit, vpa.nombres, t.id
                FROM v_personal_activo  vpa
                INNER JOIN terceros t ON t.nit = vpa.nit
                WHERE vpa.nombres LIKE :valor;
            """
            query = self.db.execute(text(sql), {"valor": f"%{valor}%"}).fetchall()

            # Retornar directamente una lista de diccionarios
            # return [{"cedula": key[0], "nombres": key[1]} for key in query] if query else []
            return [{"id": key.id, "nit": key.nit, "nombres": key.nombres} for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException("Error al obtener personal.")
        finally:
            self.db.close()

    # Query para consultar registros y datos de formaciones
    def consultar_datos(self, data: dict):
        
        try:
            codigo = data["codigo"]
            tema = data["tema"]
            macroproceso = data["macroproceso"]
            usuario = data["usuario"]
            nivel_formacion = data["nivel_formacion"]
            tipo_actividad = data["tipo_actividad"]
            modalidad = data["modalidad"]
            estado_formacion = data["estado_formacion"]
            fecha_desde = data["fecha_desde"]
            fecha_hasta = data["fecha_hasta"]
            cant_registros = 0
            limit = data["limit"]
            position = data["position"]
            horas = 0
            minutos = 0

            response = list()
            
            sql = """
                SELECT COUNT(*) OVER() AS total_registros, rgf.id, rgf.codigo, nf.nombre as nivel_formacion, ta.nombre as tipo_actividad, rgf.tema,
                mo.nombre as modalidad, ef.nombre as estado_formacion, vpa.nombres as nombre_personal, m.nombre as macroproceso,
                rgf.fecha_inicio, rgf.fecha_fin, rgf.duracion_horas, rgf.duracion_minutos, vpa.nit as cedula, rgf.evaluacion,
                cf.nota_eva_escrita, cf.nota_eva_practica, cf.nota_eva_interactiva
                FROM dbo.registro_general_formacion rgf
                INNER JOIN tipo_nivel_formacion nf ON nf.id = rgf.nivel_formacion AND nf.estado = 1
                INNER JOIN tipo_actividad ta ON ta.id = rgf.tipo_actividad AND ta.estado = 1
                INNER JOIN tipo_modalidad mo ON mo.id = rgf.modalidad AND mo.estado = 1
                INNER JOIN estado_formacion ef ON ef.id = rgf.estado_formacion AND ef.estado = 1
                INNER JOIN personal_formacion_detalle pfd on pfd.formacion_id = rgf.id AND pfd.estado = 1
                INNER JOIN v_personal_activo vpa on vpa.nit = pfd.nit
                INNER JOIN macroprocesos_cargos mc on mc.cargo_y_personal = vpa.cargo
                INNER JOIN macroprocesos m on m.id = mc.macroproceso_id
                LEFT JOIN calificaciones_formacion cf ON cf.formacion_id = rgf.id AND cf.cedula = vpa.nit AND cf.estado = 1
                WHERE rgf.estado = 1 
            """

            if codigo:
                sql = self.add_codigo_query(sql, codigo)
            if tema:
                sql = self.add_tema_query(sql, tema)
            if macroproceso:
                sql = self.add_macroproceso_query(sql, macroproceso)
            if usuario:
                sql = self.add_usuario_query(sql, usuario)
            if nivel_formacion:
                sql = self.add_nivel_formacion_query(sql, nivel_formacion)
            if tipo_actividad:
                sql = self.add_tipo_actividad_query(sql, tipo_actividad)
            if modalidad:
                sql = self.add_modalidad_query(sql, modalidad)
            if estado_formacion:
                sql = self.add_estado_formacion_query(sql, estado_formacion)
            if fecha_desde and fecha_hasta:
                sql = self.add_fechas_query(
                    sql, 
                    fecha_desde, 
                    fecha_hasta
                )
            
            new_offset = self.obtener_limit(limit, position)
            self.query_params.update({"offset": new_offset, "limit": limit})
            sql = sql + " ORDER BY rgf.id ASC OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY;"

            if self.query_params:
                query = self.db.execute(text(sql), self.query_params).fetchall()
            else:
                query = self.db.execute(text(sql)).fetchall()

            if query:
                cant_registros = query[0][0]
                for index, key in enumerate(query):
                    if key[12] != 0:
                        horas = key[12]
                    if key[13] != 0:
                        minutos = key[13]
                    duracion = f"{horas} horas {minutos} minutos"
                    
                    if not key[16] and not key[17] and not key[18]:
                        resumen_notas = " - "
                    else:
                        resumen_parts = []
                        if key[16] is not None:
                            resumen_parts.append(f"ESCRÍTA: {key[16]}")
                        if key[17] is not None:
                            resumen_parts.append(f"PRÁCTICA: {key[17]}")
                        if key[18] is not None:
                            resumen_parts.append(f"INTERACTIVA: {key[18]}")
                        
                        resumen_notas = " | ".join(resumen_parts)
                    
                    response.append({
                        "id": key[1],
                        "codigo": key[2],
                        "nivel_formacion": key[3],
                        "tipo_actividad": key[4],
                        "tema": key[5],
                        "modalidad": key[6],
                        "estado_formacion": key[7],
                        "nombre": key[8],
                        "macroproceso": key[9],
                        "fecha_inicio": str(key[10]) if key[10] else '',
                        "fecha_fin": str(key[11]) if key[11] else '',
                        "duracion": duracion,
                        "cedula": key[14],
                        "evaluacion": json.loads(key[15]),
                        "nota_eva_escrita": key[16],
                        "nota_eva_practica": key[17],
                        "nota_eva_interactiva": key[18],
                        "resumen_notas": resumen_notas
                    })
            result = {"registros": response, "cant_registros": cant_registros}
            return result
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Función para agregar condición de código a la consulta principal
    def add_codigo_query(self, sql, codigo):
        sql = sql + " AND rgf.codigo LIKE :codigo"
        self.query_params.update({"codigo": f"%{codigo}%"})
        return sql

    # Función para agregar condición de tema a la consulta principal
    def add_tema_query(self, sql, tema):
        sql = sql + " AND rgf.tema LIKE :tema"
        self.query_params.update({"tema": f"%{tema}%"})
        return sql

    # Función para agregar condición de macroproceso a la consulta principal
    def add_macroproceso_query(self, sql, macroproceso):
        sql = sql + " AND m.id = :macroproceso"
        self.query_params.update({"macroproceso": macroproceso})
        return sql

    # Función para agregar condición de usuario a la consulta principal
    def add_usuario_query(self, sql, usuario):
        sql = sql + " AND vpa.nit = :usuario"
        self.query_params.update({"usuario": usuario})
        return sql

    # Función para agregar condición de nivel formación a la consulta principal
    def add_nivel_formacion_query(self, sql, nivel_formacion):
        sql = sql + " AND rgf.nivel_formacion = :nivel_formacion"
        self.query_params.update({"nivel_formacion": nivel_formacion})
        return sql

    # Función para agregar condición de tipo actividad a la consulta principal
    def add_tipo_actividad_query(self, sql, tipo_actividad):
        sql = sql + " AND rgf.tipo_actividad = :tipo_actividad"
        self.query_params.update({"tipo_actividad": tipo_actividad})
        return sql

    # Función para agregar condición de modalidad a la consulta principal
    def add_modalidad_query(self, sql, modalidad):
        sql = sql + " AND rgf.modalidad = :modalidad"
        self.query_params.update({"modalidad": modalidad})
        return sql

    # Función para agregar condición de estado de formación a la consulta principal
    def add_estado_formacion_query(self, sql, estado_formacion):
        sql = sql + " AND rgf.estado_formacion = :estado_formacion"
        self.query_params.update({"estado_formacion": estado_formacion})
        return sql

    # Función para agregar condición de fechas a la consulta principal
    def add_fechas_query(self, sql, fecha_desde, fecha_hasta):
        sql = sql + " AND rgf.fecha_inicio BETWEEN :fecha_desde AND :fecha_hasta"
        self.query_params.update({"fecha_desde": fecha_desde, "fecha_hasta": fecha_hasta})
        return sql

    # Función para obtener el limite de para paginar
    def obtener_limit(self, limit: int, position: int):
        offset = (position - 1) * limit
        return offset
    
    # Query para obtener los origenes de necesidad
    def get_origen_necesidad(self):

        try:
            query = self.db.query(
                TipoOrigenNecesidadModel
            ).filter(
                TipoOrigenNecesidadModel.estado == 1
            ).all()                 

            # Retornar directamente una lista de diccionarios
            return [{"id": key.id, "nombre": key.nombre} for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para obtener los tipos de evaluación
    def get_tipo_evaluacion(self):

        try:
            query = self.db.query(
                TipoEvaluacionModel
            ).filter(
                TipoEvaluacionModel.estado == 1
            ).all()                 

            # Retornar directamente una lista de diccionarios
            return [{"id": key.id, "nombre": key.nombre} for key in query] if query else []
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para obtener los tipos de evaluación
    def get_evaluaciones_by_id(self, evaluacion_ids: list):

        try:
            query = self.db.query(
                TipoEvaluacionModel.nombre
            ).filter(
                TipoEvaluacionModel.id.in_(evaluacion_ids)
            ).all()
            
            return "\n".join([f"- {key.nombre}" for key in query]) if query else ""
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para buscar si existen calificaciones y actualizarlas.
    def buscar_y_actualizar_calificacion(self, data: dict):
        
        try:
            query = self.db.query(
                CalificacionesFormacionModel
            ).filter(
                CalificacionesFormacionModel.formacion_id == data["formacion_id"],
                CalificacionesFormacionModel.cedula == data["cedula"]
            ).first()
            
            if query:
                query.nota_eva_escrita = data["nota_eva_escrita"]
                query.nota_eva_practica = data["nota_eva_practica"]
                query.nota_eva_interactiva = data["nota_eva_interactiva"]
                self.db.commit()        
                return True
                
        except Exception as ex:
            print(str(ex))
            raise CustomException(str(ex))
        finally:
            self.db.close()

    # Query para buscar si existen calificaciones y actualizarlas.
    def guardar_calificacion(self, data: dict):
        
        try:
            calificacion = CalificacionesFormacionModel(data)
            self.db.add(calificacion)
            self.db.commit()
            return True
        except Exception as ex:
            raise CustomException(str(ex))
        finally:
            self.db.close()
