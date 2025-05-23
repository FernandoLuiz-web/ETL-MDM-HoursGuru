"""
    Clockify
"""
CLOCKIFY_LOGGER_PROJECTS = "Obter os projetos no clockify para obter o ID."
CLOCKIFY_LOGGER_ACTIVE_USERS = "Obter os usuários ativos presentes no clockify."
CLOCKIFY_LOGGER_APPOINTMENTS = "Obter os apontamentos efetuados para os projetos."

"""
    Dataverse
"""
DATAVERSE_LOGGER_PROJECTS_PLANNING = "Obter os planejamentos mensais de projetos."

"""
    Pymongo
"""
PYMONGO_LOGGER_CONNECT_HOST = "Configurando conexão com o host mongodb."
PYMONGO_LOGGER_CONNECT_DATABASE = "Conexão com banco de dados estabelecida."
PYMONGO_LOGGER_GET_COLLECTION = "Obter collection do banco mongodb."
PYMONGO_LOGGER_CLOSE_CONNECTION = "Fechando a conexão com o banco de dados mongodb."

"""
    ETL clockify
"""
ETL_CLOCKIFY_LOGGER_GET_ACTIVE_PROJECTS_ID = "Obter lista de id's de projeto do clockify que estão ativos."
ETL_CLOCKIFY_LOGGER_GET_APPOINTMENTS_PER_DAY =  "Obter os apontamentos de projetos ativos no range de um dia."
ETL_CLOCKIFY_LOGGER_EXTRACT = "Extração dos dados do clockify."
ETL_CLOCKIFY_LOGGER_TRANSFORM = "Transformar os dados do clockify obtidos na extração."
ETL_CLOCKIFY_LOGGER_LOAD =  "Carregar dados do clockify normalizados pela transformação, no document mongodb."
ETL_CLOCKIFY_LOGGER_RUN = "Rodar a ETL etl_clockify."

"""
    ETL dataverse
"""
ETL_DATAVERSE_LOGGER_EXTRACT = "Extração dos dados do dataverse."
ETL_DATAVERSE_LOGGER_TRANSFORM = "Transformar os dados do dataverse obtidos na extração."
ETL_DATAVERSE_LOGGER_LOAD = "Carregar dados do dataverse normalizados pela transformação, no document mongodb."
ETL_DATAVERSE_LOGGER_RUN = "Rodar a ETL etl_dataverse"

"""
    AppointedHours_repository
"""
GET_ALL_APPOINTED_HOURS_LOGGER_MSG = "Requisição para obter todos os apontamentos de horas."
GET_APPOINTED_FOR_ID_LOGGER_MSG = "Requisição para obter um apontamento de horas por id."
INSERT_NEW_APPOINTMENT_LOGGER_MSG = "Inserindo registros de apontamento no banco de dados."
DELETE_APPOINTMENT_FOR_ID_LOGGER_MSG = "Deletando registro de um apontamento no banco de dados."
DELETE_ALL_APPOINTMENTS_IN_DOCUMENT = "Deletando todos os registros de apontamento no banco de dados."
"""
    ProjectPlanning_repository
"""
GET_ALL_PPLANNING_LOGGER_MSG = "Requisição para obter todos os planejamentos de projetos."
GET_PPLANNING_FOR_ID_LOGGER_MSG = "Requisição para obter um planejamento pelo id."
INSERT_NEW_PPLANNINGS_LOGGER_MSG = "Inserindo novos registros de planejamento no banco de dados."
DELETE_ALL_PPLANNING_LOGGER_MSG = "Deletando todos os planejamentos de projetos do banco de dados."