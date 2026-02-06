import os
import re

import shortuuid
from openpyxl.worksheet.worksheet import Worksheet
from unidecode import unidecode

import constants


def generate_short_code() -> str:
    return str(shortuuid.uuid())


def convert_to_date_format(date_to_be_converted, converted_format=constants.DATE_FORMAT) -> str:
    return date_to_be_converted.strftime(converted_format)


def is_excel(filename: os.DirEntry) -> bool:
    extension = os.path.splitext(filename)[1][1:]
    if extension in ["xlsx", "xls"]:
        return True
    return False


def get_visible_sheets(sheets: [Worksheet]):
    visible_sheets = []
    for sheet in sheets:
        if sheet.sheet_state == "visible":
            visible_sheets.append(sheet)

    return visible_sheets


def cleanup(name):
    return name


def get_dist_name(dist_name) -> str:
    if dist_name.lower() == "nacaroa":
        dist_name = "NACR"
    elif dist_name.lower() == "nacalaavelha" or dist_name.lower() == "nacala a velha":
        dist_name = "NAVE"
    else:
        dist_name = dist_name[:4].upper()
    return dist_name
