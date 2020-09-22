from fastapi import FastAPI
import pandas as pd
import asyncio
import aiohttp
from pandas import ExcelWriter
from starlette.responses import StreamingResponse
from io import StringIO, BytesIO

specializations = {
    'Администратор баз данных': {
        'specialization': '1.420',
    },
    'Системный администратор информационно-коммуникационных систем': {
        'specialization': '25.383',
    },

    'Программист': {
        'specialization': '1.221',
    },

    'Специалист по тестированию в области информационных технологий': {
        'specialization': '1.117',
    },

    'Специалист по информационным системам': {
        'specialization': '14.91',
    },

    'Системный программист': {
        'specialization': ['1.272', '1.273'],
    },

    'Специалист по администрированию сетевых устройств информационно-коммуникационных систем': {
        'specialization': '1.270',
        'text': 'администратор'
    },

    'Специалист по дизайну графических и пользовательских интерфейсов': {
        'text': 'дизайн интерфейсов',
    },

    'Системный аналитик': {
        'specialization': '1.25',
    },

    'Руководитель проектов в области информационных технологий': {
        'specialization': '1.327',
    },
    'Технический писатель': {
        'specialization': '1.296',
    },
    'Архитектор программного обеспечения': {
        'text': 'System Architect'
    }
}

soft_skills = [
    ['Коммуникативные навыки', 'Информаирование сотрудников', 'Проведение собеседований', 'Проводить интервью',
     'Проводить презентации', 'Консультирование', 'Обратная связь с заказчиком', 'Переговоры'],
    ['Работа в команде', 'Распределение задач', 'Командная работа'],
    ['Самоорганизация', 'Самообразование', 'Способность к непрерывному обучению', 'Самостоятельность',
     'Структурировать собственные знания'],
    ['Критическое мышление', 'Аналитическое мышление', 'Системное мышление', 'Выбор стратегии',
     'Выявление значимых характеристик', 'Анализ'],
    ['Планирование', 'Управление проектами', 'Распределение задач'],
    ['Межкультурное взаимодействие'],
    ['Эмоциональный интеллект', 'Эмпатия', 'Сопереживание'],
    ['Адаптивность', 'Работа в условиях неопределенности'],
    ['Организованность', 'Тайм-менеджмент'],
    ['Достижение результатов', 'Ответственность', 'Принятие риска', 'Инициативность', 'Настойчивость',
     'Достижение целей', 'Распределение задач', 'Принятие решений'],
    ['Обучение других', 'Наставничество'],
    ['Разработка и реализация проектов', 'Распределение задач', 'Осуществление контроля', 'Коммуникации',
     'Планирование']
]

default_params = {
    'experience': 'between1And3',
    'per_page': 0,
    'page': 0,
}


async def async_request(url, params):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
        async with session.get(url, params=params) as resp:
            return await resp.json()


async def get_count_by_soft_skill(skill, params):
    synonyms = ' OR '.join([str(x) for x in skill])
    if params.get('text') is not None:
        synonyms = f'({synonyms}) AND {params.get("text")}'
    params.update({'text': synonyms})
    r = await async_request("https://api.hh.ru/vacancies", params)
    found_by_skill_number = r['found']
    return skill[0], found_by_skill_number


async def get_vacancies_number(profession):
    current_params = default_params.copy()
    current_params.update(specializations[profession])
    r = await async_request("https://api.hh.ru/vacancies", current_params)
    found_number = r['found']
    return profession, current_params, found_number


async def get_matrix():
    matrix = dict()
    futures = [get_vacancies_number(k) for k, _ in specializations.items()]
    for future in asyncio.as_completed(futures):
        profession, params, found_number = await future
        matrix[profession] = dict()
        matrix[profession]['Всего'] = found_number
        furure_skills = [get_count_by_soft_skill(skill, params.copy()) for skill in soft_skills]
        for future_skill in asyncio.as_completed(furure_skills):
            skill, found_by_skill_number = await future_skill
            matrix[profession][skill] = found_by_skill_number
    return matrix


def convert_to_file(matrix):
    df = pd.DataFrame(matrix)
    excel_file = BytesIO()
    writer = ExcelWriter(excel_file, engine='xlsxwriter')
    df.to_excel(writer)
    writer.save()
    data = excel_file.getvalue()
    return data


app = FastAPI(docs_url="/")


@app.get("/russia", response_class=StreamingResponse)
async def russia():
    """ Статистика по России """
    default_params['area'] = 113
    headers = {
        'Content-Disposition': 'attachment; filename="russia.xlsx"'
    }

    matrix = await get_matrix()
    data = convert_to_file(matrix)
    return StreamingResponse(iter([data]), headers=headers)


@app.get("/hmao", response_class=StreamingResponse)
async def hmao():
    """ Статистика по ХМАО """
    default_params['area'] = 1368
    headers = {
        'Content-Disposition': 'attachment; filename="hmao.xlsx"'
    }
    matrix = await get_matrix()
    data = convert_to_file(matrix)
    return StreamingResponse(iter([data]), headers=headers)

