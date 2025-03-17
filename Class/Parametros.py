from Utils.tools import Tools, CustomException
from Utils.querys import Querys

class Parametros:

    def __init__(self, db):
        self.db = db
        self.tools = Tools()
        self.querys = Querys(self.db)
        self.CORPORATIVA = 1
        self.ROL = 2
        self.POSICION = 3

    def get_parametros(self):
        """ Api que realiza la consulta de los estados. """

        try:

            # Iniciamos listas vacías
            data_response = dict()
            nivel_formacion = list()
            tipo_actividad = list()
            ciudades_formacion = list()
            competencia_corporativa = list()
            competencia_rol = list()
            competencia_posicion = list()
            macroprocesos = list()
            

            # Llamamos a la función de consultar get_nivel_formacion
            nivel_formacion = self.querys.get_nivel_formacion()

            # Llamamos a la función de consultar get_tipo_actividad
            tipo_actividad = self.querys.get_tipo_actividad()

            # Llamamos a la función de consultar get_ciudades_formacion
            ciudades_formacion = self.querys.get_ciudades_formacion()
        
            # Llamamos a la función de consultar tipos_competencia_formacio
            competencia_corporativa = self.querys.tipos_competencia_formacion(
                self.CORPORATIVA)
        
            # Llamamos a la función de consultar tipos_competencia_formacio
            competencia_rol = self.querys.tipos_competencia_formacion(
                self.ROL)
        
            # Llamamos a la función de consultar tipos_competencia_formacio
            competencia_posicion = self.querys.tipos_competencia_formacion(
                self.POSICION)

            # Llamamos a la función de consultar get_macroprocesos
            macroprocesos = self.querys.get_macroprocesos()

            # Llamamos a la función de consultar get_modalidad
            modalidad = self.querys.get_modalidad()

            # Armamos el diccionario de salida
            data_response.update({
                "nivel_formacion": nivel_formacion,
                "tipo_actividad": tipo_actividad,
                "ciudades_formacion": ciudades_formacion,
                "competencia_corporativa": competencia_corporativa,
                "competencia_rol": competencia_rol,
                "competencia_posicion": competencia_posicion,
                "macroprocesos": macroprocesos,
                "tipo_modalidad": modalidad
            })

            # Retornamos la información.
            return self.tools.output(200, "Datos encontrados.", data_response)

        except Exception as e:
            print(f"Error al obtener información de tercero: {e}")
            raise CustomException("Error al obtener información de tercero.")

    def get_proveedores(self, data: dict):
        """ Api que realiza la consulta de los estados. """

        try:

            valor = data["valor"]

            # Acá usamos la query para traer la información get_proveedores
            proveedores = self.querys.get_proveedores(valor)

            # Retornamos la información.
            return self.tools.output(200, "Datos encontrados.", proveedores)

        except Exception as e:
            print(f"Error al obtener información de tercero: {e}")
            raise CustomException("Error al obtener información de tercero.")
