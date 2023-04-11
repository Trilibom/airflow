"""
Event Converter

Генерация событий о дислокациях на базе основных таблиц из ЦИТТРАНС. Запускается
по триггеру после процесса обновления cittrans_update_v3. После окончания обновления
в буферных таблицах остается порция новых строк. По этим буферным таблицам выполняется
запрос, и результат этого запроса сохраняется во временную таблицу, которая затем
построчно преобразовывается в отдельные сообщения `equipment_event.update` для
Сервиса Дислокаций.


CONNECTIONS
 - clickhouse_staging:  база источник
 - mongo_dislocation:   база назначение

VARIABLES
 - event_converter_mongo_collection:    название коллекции в Сервисе Дислокаций
 - event_converter_mongo_batch_size:    размер пакетной записи
 - event_converter_last_block_id:       счетчик блоков, прибавляется с каждым запуском


ISSUE
ESB-1867: Event Converter. Генерация эвентов в дислокацию на основе операций из cit
https://youtrack100.trcont.ru/issue/ESB-1867

ATTENTION
В случае падения дага dislocation_event_converter упавшие запуски следует удалить.
Далее необходимо перезапустить cittrans_update
для перезагрузки данных на этот момент времени (а именно обновления sf_change_date),
т.к. забор данных dislocation_event_converter осуществляется по условию:
toDateTime(sf_change_date) >= toTimezone(toDateTime('{{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}', 'UTC'), 'Europe/Moscow')
AND toDateTime(sf_change_date) < toTimezone(toDateTime('{{ (execution_date + macros.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S") }}', 'UTC'), 'Europe/Moscow')
Когда наступит интервал execution_date на момент sf_change_date - обновления данных cittrans_update -
недостающие данные подхватятся.
Также следует предупредить коллег из дислокаций о поломке и будущем приходе большого пакета данных.
"""

import os
import uuid
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.clickhouse.operators.clickhouse import ClickHouseOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
import pymongo
import logging

from utils.priority import first_priority


with DAG(
    dag_id='dislocation_event_converter_v1.0.0',
    tags=["dislocation"],
    default_args=first_priority,
    start_date=days_ago(1),
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
) as dag:

    MONGO_COLLECTION = Variable.get("event_converter_mongo_collection", default_var="events")
    MONGO_BATCH_SIZE = int(Variable.get("event_converter_mongo_batch_size", default_var="100"))


    # Таблицы трекинга сломанных вагонов и контейнеров
    create_tables = []
    for table in ["dict_container_faulty", "dict_wagon_faulty"]:
        create_tables.append(
            ClickHouseOperator(
                task_id=f"create_{table}",
                clickhouse_conn_id="clickhouse_staging",
                sql="sql/v3/create_tracking.sql",
                params={ "table": table },
            )
        )


    # Временная таблица преобразования в события дислокаций
    drop_buffer = ClickHouseOperator(
        task_id=f"create_dislocation_buffer",
        clickhouse_conn_id="clickhouse_staging",
        sql="DROP TABLE IF EXISTS cittrans__dislocation_buffer",
    )


    # Получение событий дислокаций из основных таблиц ЦИТТРАНС
    select_dislocations = ClickHouseOperator(
        task_id=f"select_dislocations",
        clickhouse_conn_id="clickhouse_staging",
        sql=(
            """DROP TABLE IF EXISTS stoppin;""",
            f"sql/v3/01-create-stopping.sql",

            """DROP TABLE  IF EXISTS stoppin1;""",
            f"sql/v3/02-create-stopping.sql",

            """DROP TABLE  IF EXISTS stoppin2;""",
            f"sql/v3/03-create-stopping.sql",

            """DROP TABLE  IF EXISTS stoppin3;""",
            f"sql/v3/04-create-stopping.sql",

            """DROP TABLE  IF EXISTS stoppin4;""",
            f"sql/v3/05-create-stopping.sql",

            f"sql/v3/select_dislocations.sql",
        )
    )


    # Преобразование строк в события дислокаций
    @task
    def generate_event_messages():
        hook = BaseHook.get_hook("clickhouse_staging")

        # mongo = MongoHook(conn_id="mongo_dislocation_v3")
        # collection = mongo.get_collection(mongo_collection=MONGO_COLLECTION)
        #
        # Учетка выдана на базу admin.
        # Пэтому работаем напрямую через pymongo.

        conn = BaseHook.get_hook("mongo_dislocation_v3").connection
        mongo = pymongo.MongoClient(f"mongodb://{conn.login}:{conn.password}@{conn.host}")
        collection = mongo[conn.schema][MONGO_COLLECTION]
        total = collection.count_documents({})

        logging.info(f'Total dislocation events in collection "{MONGO_COLLECTION}": {total}')

        # Получаем сразу все таблицы неисправных контейнеров и вагонов (храним только состояние "неисправный")
        wagon_faulty_table = hook.get_records("SELECT * FROM dict_wagon_faulty")
        container_faulty_table = hook.get_records("SELECT * FROM dict_container_faulty")

        wagon_faulty = {key:status for (key, status) in wagon_faulty_table if status}
        container_faulty = {key:status for (key, status) in container_faulty_table if status}

        # 1. Фильтруем записи из таблицы операций по контейнерам (cont_oper)
        total = hook.get_first(f"SELECT count(*) FROM cittrans__dislocation_buffer")[0]
        logging.info(f"Total records in dislocation buffer: {total}")

        logging.info("Get all records from dislocation buffer")
        records = hook.get_records(f"""
            SELECT *
            FROM cittrans__dislocation_buffer
        """)

        block_id = int(Variable.get("event_converter_last_block_id", default_var="0")) + 1

        logging.info("Starts converting rows to events")
        count = 0
        items = []
        for record in records:
            (
                actual,
                date_time,
                transport_mode,
                status__id,
                status__in_movement,
                status__name,
                document__type_id,
                document__type_name,
                document__number,
                document__date,
                document__url,
                source__system,
                source__object,
                source__code,
                order__id,
                shipment_type_id,
                shipper__id,
                catalog_shipper_name,
                shipper__name,
                shipper__okpo,
                shipper__tgnl,
                consignee__id,
                catalog_consignee_name,
                consignee__name,
                consignee__okpo,
                consignee__tgnl,
                order__cargo_info__is_empty,
                order__cargo_info__description,
                order__cargo_info__etsng_id,
                order__cargo_info__gng_id,
                order__cargo_info__is_danger,
                order__cargo_info__package_code,
                order__cargo_info__weight_mt,
                equipment__eta,
                equipment__container__id,
                iso_6346,
                size_type,
                teu,
                equipment__container__faulty,
                equipment__container__renter_name,
                equipment__wagon__id,
                equipment__wagon__model,
                equipment__wagon__teu,
                equipment__wagon__faulty,
                service_info__registration_date,
                next_repair_date,
                station_id,
                latitude,
                longitude,
                region_id,
                country_id,
                position__source_system,
                position__source_object,
                position__source_code,
                rail_info__cont_train,
                waybill_number,
                waybill__stations__from__border,
                waybill__stations__from__station_id,
                waybill__stations__to__border,
                waybill__stations__to__station_id
            ) = record


            # 2. Если код события 55-0 добавляем запись в container_faulty,
            # если 56-0 убираем из container_faulty
            # if container_operation_code == '55-0':
            #     container_faulty[equipment__container__number] = True

            # if container_operation_code == '56-0':
            #     del container_faulty[equipment__container__number]


            # 3. На основании vagID проверяем операцию по вагону по той же станции,
            # если статус вагонной операции 82-0 добавляем в wagon_faulty, если 83-0
            # убираем из wagon_faulty
            # if wagon_operation_code == '82-0':
            #     wagon_faulty[equipment__wagon__number] = True

            # if wagon_operation_code == '83-0':
            #     del wagon_faulty[equipment__wagon__number]

            # WARNING: алсо есть статус `equipment__wagon__faulty` - как его учитывать?


            # 4. Формируем евент с учетом мэппинга (формат equipment_event.update)
            # Закоментированы поля, которые нам либо не нужны для связывания, либо требуют уточнения
            item = {
                # Новые поля
                "block_id": block_id,                                                         # Номер блока
                "message_id": str(uuid.uuid4()),                                              # Уникальный id сообщения
                # Поля из оригинального формата
                "actual": actual,                                                             # Статус события (true - реальное событие, false - надо отменить), если false, то обязательное поле только source_id
                "datetime": date_time,                                                        # Дата и Время события
                "transport_mode": transport_mode,                                             # Тип плеча, на котором произошло событие
                "status": {                                                                   # Статус по результатам события
                    "id": status__id,                                                         # Идентификатор статуса в справочнике
                    "in_movement": status__in_movement,                                       # находится в движении (может быть null)
                    "name": status__name,                                                     # Наименование статуса (может быть null)
                },
                "document": {                                                                 # Документ-основание для события (может быть null)
                    "type": {                                                                 # Тип документа
                        "id": document__type_id,                                              # Идентификатор типа события в справочнике
                        "name": document__type_name                                           # Наименование типа события (может быть null)
                    },
                    "number": document__number,                                               # Номер документв
                    "date": document__date,                                                   # Дата документа
                    # "url": document__url                                                       # Ссылка на документ в хранилище S3 (может быть null)
                },
                "source": {                                                                   # Ссылка на код события в системе-источнике (может быть null)
                    "system": source__system,                                                 # Система-источник
                    "object": source__object,                                                 # Тип объекта во внешнем источнике
                    "code": source__code,
                    #"id": source__id,                                                        # Значение ссылки (код объекта во внешнем источнике)
                },
                "order": {                                                                    # Данные о заказе ТК (может быть null)
                    "id": order__id,                                                          # Идентификатор заказа (может быть null)
                    # "location": {
                        # "total_distance": 3361,                                             # Расстояние между начальным и конечным пунктом заказа (может быть null)
                        #"from": {                                                            # Пункт отправления (может быть null)
                            #"country_id": from__country_id,                                  # Идентификатор страны отправления (может быть null)
                            # "catalog_id": "T_CN_MUD", # ???                                 # Идентификатор пункта отправления (может быть null)
                            # "station_id": from__station_id,                                 # Иденнтификатор станции отправления (может быть null)
                            # "address": null # ???                                           # Адрес пункта отправления (может быть null)
                        # },
                        # "via":  null, # ???                                                 # Не используется
                        #"to": {                                                              # Пункт назначения (может быть null)
                            # "country_id": to__country_id,                                   # Идентификатор страны назначения (может быть null)
                            # "catalog_id": "T_RU_KLE_TK", # ???                              # Идентификатор пункта назначения (может быть null)
                            # "station_id": to__station_id,                                   # Иденнтификатор станции назначения (может быть null)
                            # "address": {                                                    # Адрес пункта назначения (может быть null)
                            #     "str": "Москва, ул. Павлова, 24, стр. 1/2",                 # Адрес одной строкой
                            #     "kladr_id": "3201000000000",                                # КЛАДР (может быть null)
                            #     "fias_id": "5ee84ac0-eb9a-4b42-b814-2f5f7c27c255",          # ФИАС (может быть null)
                            #     "city": "Москва"                                            # Название города (может быть null)
                            # }
                       # }
                    #},
                    "shipment_type_id": shipment_type_id,                                     # Вид перевозки (3000 - контейнерные перевозки, 1000 - вагонные)
                    "shipment": {                                                             # Поставка по заказу (контейнер/вагон на плече)
                        # "id": "fhgbgu6",                                                    # Идентификатор поставки (может быть null)
                        # "leg_id":  12345,                                                   # Идентификатор плеча маршрута (может быть null)
                        # "points": {
                        #     "total_distance": 3361,                                         # Расстояние между начальной и конечной точкой плеча (может быть null)
                        #     "from": {                                                       # Начальная точка плеча (может быть null)
                        #         "border": False,                                            # Признак погранперехода (может быть null)
                        #         "catalog_id": "T_RU_ZAB_TK",                                # Идентификатор пункта начала плеча
                        #         "station_id": "947005"                                      # Иденнтификатор станции начала плеча (может быть null)
                        #     },
                        #     "to": {                                                         # Конечная точка плеча (может быть null)
                        #         "border": False,                                            # Признак погранперехода (может быть null)
                        #         "catalog_id": "T_RU_KUN_TK",                                # Идентификатор пункта окончания плеча
                        #       "station_id": "181808"                                        # Иденнтификатор станции окончания плеча (может быть null)
                        #     }
                        # },
                        # "etd": "2020-10-17T03:30:00+03:00",                                 # Ожидаемое (расчетное) время отправления (может быть null)
                        # "atd": None,                                                        # Фактическое время отправления (может быть null)
                        # "eta": None,                                                        # Ожидаемое (расчетное) время прибытия (может быть null)
                        # "ata": None,                                                        # Фактическое время прибытия (может быть null)
                        "shipper": {                                                          # Грузоотправитель на плече (может быть null)
                            "id": shipper__id,                                                # ЕКК грузоотправителя (может быть null)
                            "name": shipper__name,                                            # Наименование грузоотправителя (может быть null)
                            "okpo": shipper__okpo,                                            # Код ОКПО грузоотправителя (может быть null)
                            "tgnl": shipper__tgnl,                                            # Код ТГНЛ грузоотправителя (может быть null)
                            # "address": {                                                    # Адрес грузоотправителя (может быть null)
                            #     "str": "China, SHANGHAI, RM ...",                           # Адрес одной строкой
                            #     "kladr_id": null,                                           # КЛАДР (может быть null)
                            #     "fias_id": null ,                                           # ФИАС (может быть null)
                            #     "country_id": "156",                                        # Код страны (может быть null)
                            #     "city": "Шанхай"                                            # Название города (может быть null)
                            # },
                            # "source": {                                                     # Ссылка на идентификатор грузоотправителя во внешней системе (может быть null)
                            #     "system": "etran",                                          # Система-источник
                            #     "object": "org",                                            # Тип объекта во внешнем источнике
                            #     "code": "12345"                                             # Значение ссылки (идентификатор объекта во внешнем источнике)
                            # }
                         },
                          "consignee": {                                                      # Грузополучатель на плече (может быть null)
                             "id": consignee__id,                                             # ЕКК грузополучателя (может быть null)
                             "name": consignee__name,                                         # Наименование грузополучателя (может быть null)
                             "okpo": consignee__okpo,                                         # Код ОКПО грузополучателя (может быть null)
                             "tgnl": consignee__tgnl,                                         # Код ТГНЛ грузополучателя (может быть null)
                            # "address": {                                                    # Адрес грузополучателя (может быть null)
                            #     "str": "119121, г Москва, ул Бурденко, д 11",               # Адрес одной строкой
                            #     "kladr_id": "7777770000000",                                # КЛАДР
                            #     "fias_id": "5ee84ac0-eb9a-4b42-b814-2f5f7c27c255",          # ФИАС
                            #     "country_id": "643",                                        # Код страны
                            #     "city": "Москва"                                            # Название города
                            # },
                            # "source": {                                                     # Ссылка на идентификатор грузоотправителя во внешней системе (может быть null)
                            #     "system": "etran",                                          # Система-источник
                            #     "object": "org",                                            # Тип объекта во внешнем источнике
                            #     "code": "12345"                                             # Значение ссылки (идентификатор объекта во внешнем источнике)
                            # }
                          }
                    },
                     "cargo_info": {                                                          # Данные о перевозимом грузе (может быть null)
                        "is_empty": order__cargo_info__is_empty,                              # Признак порожней перевозки
                        "description": order__cargo_info__description,                        # Наименование груза (может быть null)
                        "etsng_id": order__cargo_info__etsng_id,                              # Код ЕТСНГ (может быть null)
                        "gng_id": order__cargo_info__gng_id,                                  # Код ГНГ (может быть null)
                        # "previous_etsng_id": "00000",                                       # Предыдущий код ЕТСНГ (может быть null)
                        "danger": {                                                           # Признаки опасного груза (может быть null)
                            "is_danger": order__cargo_info__is_danger,
                            # "un_code": "UN_1395",                                           # Международный код опасности груза
                            # "package_type": "II"                                            # Класс упаковки опасного груза
                            # таких полей нет...
                            # но можем достать флаг is_danger
                            # В Дислокации нужны именно коды ООН, если таких нет,
                            # то не нужно
                        },
                        "package_code": order__cargo_info__package_code,                      # Идентификатор типа упаковки (может быть null)
                        "weight_mt": order__cargo_info__weight_mt,                            # Вес груза в метрических тоннах (может быть null)
                    }
                },
                "equipment": {
                    # "id": "q12376t3t7", # (deprecated)                                      # Идентификатор слота оборудования (может быть null)
                    # "sequence": "9",
                    # "etd": "2020-10-17T03:30:00+03:00",                                     # Ожидаемое (расчетное) время отправления по цепочке поставок (может быть null)
                    # "atd": None, # ???                                                      # Фактическое время отправления по цепочке поставок (может быть null)
                    "eta": equipment__eta,                                                    # Ожидаемое (расчетное) время прибытия по цепочке поставок (может быть null)
                    # "ata": None,                                                            # Фактическое время прибытия по цепочке поставок (может быть null)
                    "container": {                                                            # Данные о контейнере (может быть null)
                        "number": equipment__container__id,                                   # Идентификатор (номер) контейнера
                        #"type": equipment__container__type,                                  # Тип контейнера по справочнику ТК (может быть null)
                        "iso_6346": iso_6346,                                                 # Маркировка контейнера по ISO 6346 (может быть null)
                        #"iso668": "1AAA",                                                    # Маркировка контейнера по ISO 668 (может быть null)
                        "size_type": size_type,                                               # альтернатива iso6346: тип контейнера, полученный от внешнего поставщика данных
                        "teu": teu,                                                           # ДФЭ контейнера (может быть null)
                        # "gross_max_mt": equipment__container__gross_max_mt,                 # Максимальный допустимый вес брутто контейнера в метрических тоннах (может быть null)
                        # "tare_weight_mt": equipment__container__tare_weight_mt,             # Вес тары контейнера в метрических тоннах (может быть null)
                        "faulty": equipment__container__faulty,
                        # Трекинг контейнеров?!
                        #equipment__container__number in container_faulty,                    # Признак неисправности контейнера (может быть null)
                        "lessor": {                                                           # Арендодатель контейнера (может быть null)
                            # "id":"CMA",                                                     # ID Аренододетеля
                            "name": equipment__container__renter_name                         # Наименование арендодателя
                        },
                        # "less_condition": ""                                                # Условие сдачи, текст (может быть null)
                    },
                    "wagon": {                                                                # Данные о вагоне (может быть null)
                        "number": equipment__wagon__id,                                       # Идентификатор (номер) вагона
                        # "type": "CNT_60FT_72T",                                             # Тип вагона по справочнику ТК (может быть null)
                        "model": equipment__wagon__model,                                     # Модель вагона (может быть null)
                        "teu": equipment__wagon__teu,                                         # Вместимость вагона в ДФЭ (может быть null)
                        #"netto_max_mt": 72.0,                                                # Максимальная грузоподъемность вагона в метрических тоннах (может быть null)
                        #"tare_weight_mt": 22.0,                                              # Собственный вес вагона в метрических тоннах (может быть null)
                        # Можем достать эти поля
                        # Нужно уточнить точные названия полей ЦИТТРАНС
                        "faulty": equipment__wagon__faulty,
                        # Трекинг вагонов
                        #equipment__wagon__number in wagon_faulty,                            # Признак неисправности вагона (может быть null)
                        # # Этот блок пока не актуален
                        "service_info": {                                                     # Сервисные данные о вагоне (может быть null)
                            # "registration_date": "20.11.2012",                              # Дата регистрации вагона (может быть null)
                            # "registration_reason": {                                        # Причины регистрации (может быть null)
                            #     "id": "3",                                                  # Идентификатор причины регистрации
                            #     "name": "Регистрация после переоборудования"                # Наименование причины регистрации (может быть null)
                            # },
                            # "next_repair_type": 2,                                          # Тип следующего ремонта (0 — нет данных, 1 — деповской, 2 — капитальный, 3- конец службы)
                            "next_repair_date": next_repair_date,                             # Дата следующего ремонта (может быть null)
                            # "last_repair_date": "29.05.2018",                               # Дата последнего деповского ремонта (может быть null)
                            # "last_bigrepair_date":"29.05.2020",                             # Дата последнего капитального ремонта (может быть null)
                            # "expiration_date": "01.04.2024",                                # Срок службы вагона (может быть null)
                            # "miliage": 233,                                                 # Текущий пробег вагона в километрах (может быть null)
                            # "miliage_left": 45454                                           # Остаточный пробег вагона в километрах (может быть null)
                        }
                    }
                },
                "position": {                                                                 # Данные о текущей дислокации
                    # "left_distance_all": 3350,                                              # Оставшееся расстояние между текущей позицией и конечным пунктом заказа (может быть null)
                    # "left_distance_leg": 3345,                                              # Оставшееся расстояние до конечной точки плеча (может быть null)
                    # "location_id": "T_RU_ZAB_TK",                                           # Идентификатор пункта (может быть null)
                    "station_id": station_id,                                                 # Иденнтификатор станции (может быть null)
                    #"unlocode": position__region_unlocode,                                   # Международный код географического объекта ООН (может быть null)
                    "latitude": float(latitude),                                              # Широта (может быть null)
                    "longitude": float(longitude),                                            # Долгота (может быть null)
                    "region_id": region_id,                                                   # Идентификатор региона (может быть null)
                    "country_id": country_id,                                                 # Идентификатор страны (может быть null)
                    "source": {                                                               # Ссылка на идентификатор географического объекта в системе-источнике (может быть null)
                        "system": position__source_system,                                    # Система-источник
                        "object": position__source_object,                                    # Тип объекта во внешнем источнике
                        "code": position__source_code,                                        # Значение ссылки (идентификатор объекта во внешнем источнике)
                    }
                },
                # "comment": "подорван пол, дыра в стенке",                                   # Дополнительная информация
                "rail_info": {                                                                # Дополнительные данные о жд перевозке  (может быть null)
                    "cont_train": rail_info__cont_train,                                      # Признак контейнерного поезда
                    # "train_index": {                                                        # Индекс поезда  (может быть null)
                    #     "from": rail_info__train_index__from,
                    #     "ord": rail_info__train_index__ord,
                    #     "to": rail_info__train_index__to
                    # },
                    # "train_number": "0123",                                                 # Номер поезда (может быть null)
                    "derelict": False,                                                        # Признак брошенного поезда (может быть null)
                    "waybill": {
                        # "id": "000001323276",                                               # внутр идентификатор ЦИТ
                        # "date": "2020-10-15T00:00:00+03:00",                                # Дата накладной  (может быть null)
                        "number": waybill_number,                                             # Номер накладной
                        "stations": {
                            "from": {                                                         # Станция отправления
                                "border": waybill__stations__from__border,                    # Признак погранперехода (может быть null)
                                "station_id": waybill__stations__from__station_id             # Идентификатор станции отправления/погрузки
                            },
                            "to": {                                                           # Станция назначения
                                "border": waybill__stations__to__border,                      # Признак погранперехода (может быть null)
                                "station_id": waybill__stations__to__station_id               # Идентификатор станции назначения
                            }
                        }
                    }
                },
                # "truck_info": {                                                             # Дополнительные данные об автомобильной перевозке(может быть null)
                #    "reg_number": "T4583OX",                                                 # Номер автомобиля(может быть null)
                #    "driver": "Иванов А.В."                                                  # Данные водителя
                # },
                "processing_status": 0,
                "created": datetime.now()
            }

            # TODO:
            # Добавить дополнительные поля -- processing & processed_status
            items.append(item)
            count += 1

            if len(items) >= MONGO_BATCH_SIZE:
                collection.insert_many(items)
                logging.info(f"Loaded {len(items)} documents")
                items = []

            # TODO:
            # Фильтруем операции по вагонам(wagon_oper)
            # 1. Убираем все операции связанные с контейнерными операциями
            #      cont_oper.VagId = wagon_oper.VagId AND
            #      cont_oper.Esr_oper = wagon_oper.Esr_disl AND
            #      cont_oper.Date_Pop = wagon_oper.DatePop +/- 1 Day
            # 2. Убираем операции не по вагонам ТК на основании данных DWH(history.dict_our_wagons_kcmod)
            # 3. Если статус вагонной операции 82-0 добавляем в wagon_faulty, если 83-0 убираем из wagon_faulty
            # 4. Формируем эвенты по вагонам(формат equipment_event.update)

        # Записываем остаток
        collection.insert_many(items)
        logging.info(f"Loaded lastest of {len(items)} documents")

        logging.info(f"Total events generated: {count}")

        # Записываем обратно таблицы неисправных контейнеров и вагонов с измененными статусами
        logging.info(f"Save wagon faulty tracking")
        hook.insert_rows(table="dict_wagon_faulty", rows=[(x, True) for x in wagon_faulty])

        logging.info(f"Save container faulty tracking")
        hook.insert_rows(table="dict_container_faulty", rows=[(x, True) for x in container_faulty])


        Variable.set("event_converter_last_block_id", block_id)


    # Очищаем буферную таблицу конвертера событий дислокаций
    truncate_dislocations = ClickHouseOperator(
        task_id=f"truncate_dislocations",
        clickhouse_conn_id="clickhouse_staging",
        sql="TRUNCATE TABLE cittrans__dislocation_buffer"
    )


    create_tables >> \
        drop_buffer >> \
        select_dislocations >> \
        generate_event_messages() >> \
        truncate_dislocations
