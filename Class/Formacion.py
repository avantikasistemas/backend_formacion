from Utils.tools import Tools, CustomException
from Utils.querys import Querys
from datetime import datetime

class Formacion:

    def __init__(self, db):
        self.db = db
        self.tools = Tools()
        self.querys = Querys(self.db)
        self.FORM = 'FORM-'

    # Función para guardar una formación
    def guardar_formacion(self, data: dict):

        try:
            codigo = ''
            
            # Consultamos el numero siguiente en el consecutivo.
            num_siguiente = self.querys.buscar_numero_siguiente()
            if num_siguiente:
                codigo = f"{self.FORM}{num_siguiente}"
            data["codigo"] = codigo
            data["created_at"] = datetime.today()

            # Guardamos los datos de la formación.
            insertar_reg = self.querys.guardar_formacion(data)

            # Validamos si el registro fue exitoso, aumentamos el consecutivo.
            if insertar_reg:
                self.querys.actualizar_consecutivo(num_siguiente+1)

            # Retornamos la información.
            msg = f"Formación guardada exitosamente con el código: {codigo}"
            return self.tools.output(201, msg, data)

        except Exception as e:
            print(f"Error al guardar registro de formación: {e}")
            raise CustomException("Error al guardar registro de formación.")

    # Función para obtener las formaciones creadas
    def get_formaciones(self, data: dict):

        try:

            valor = data["valor"]

            # Acá usamos la query para traer la información get_proveedores
            formaciones = self.querys.get_formaciones(valor)

            # Retornamos la información.
            return self.tools.output(200, "Datos encontrados.", formaciones)

        except Exception as e:
            print(f"Error al obtener información de formaciones: {e}")
            raise CustomException("Error al obtener información de formaciones.")

    # Función para obtener las formaciones por id
    def get_formacion_by_id(self, data: dict):

        try:

            formacion_id = data["formacion_id"]

            # Llamamos a la función que trae la información de la formación.
            data_formacion = self.querys.get_formacion_by_id(formacion_id)

            # Retornamos la información.
            return self.tools.output(200, "Datos encontrados.", data_formacion)

        except Exception as e:
            print(f"Error al obtener información de formaciones: {e}")
            raise CustomException("Error al obtener información de formaciones.")
