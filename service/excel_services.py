import math
import os
import traceback

from os import DirEntry

import xlwings
from pandas import DataFrame
from xlwings import Book, Sheet, Range

import constants
import utils.common
from constants import LGA_CELL, BoundaryTypeEnum
from models import Boundary, User


def read_excel(provincial_boundary_code: str, excel_file_to_read: DirEntry, df: DataFrame, users_df: DataFrame,
               user_per_monitor: int) -> (
        DataFrame, DataFrame):
    with xlwings.App(visible=False):
        # @TODO Signoff from Manish for xlwings licensing
        wb = xlwings.Book(excel_file_to_read)
        sheets = return_visible_sheets(wb)
        print(sheets)
        # Assume the first visible sheet contains the boundary data
        sheet = sheets[0]
        lga_name = find_text_in_excel(LGA_CELL, sheet)
        print(f"District Name: {lga_name}")
        # @TODO 22-character limit on boundary code
        lga_boundary = Boundary(name=lga_name, boundary_type=BoundaryTypeEnum.LGA.value,
                                     parent_code=provincial_boundary_code, code=utils.common.generate_short_code(),
                                     boundary_level=constants.BoundaryLevelEnum.LGA.value)
        df = utils.common.append_to_df(df, lga_boundary)
        current_row = constants.START_AP_ROW
        max_column = sheet.api.UsedRange.Columns.Count
        max_row = sheet.api.UsedRange.Rows.Count
        print(max_row)
        ward: Boundary = None
        ward_code: str = None
        health_facility: Boundary = None
        health_facility_code: str = None
        local_monitors_at_lga = set()
        while current_row < max_row + 1:
            try:
                current_row = current_row + 1
                values = sheet.range(
                    f"{constants.START_WARD_COLUMN}{str(current_row)}:{xlwings.utils.col_name(max_column)}{str(current_row)}").value
                if values[0] is not None and values[1] is not None and lga_boundary is not None:
                    # Add Administrative Post and Locality
                    ward = get_ward(values, lga_boundary.code)
                    ward_code = ward.code
                    if len(df.query(
                            f'name.str.lower()=="{ward.name.lower()}" and boundary_type=="{ward.boundary_type}" and parent_code=="{lga_boundary.code}"')) > 0:
                        df_queried = df.query(
                            f'name.str.lower()=="{ward.name.lower()}" and boundary_type=="{ward.boundary_type}" and parent_code=="{lga_boundary.code}"')
                        print(df_queried)
                        ward_code = df_queried['code'].code
                    else:
                        df = utils.common.append_to_df(df, ward)
                    if values[2] is not None and ward is not None:
                        health_facility = get_health_facility(values, ward_code)
                        health_facility_code = health_facility.code
                        if len(df.query(
                                f'name.str.lower()=="{health_facility.name.lower()}" and boundary_type=="{health_facility.boundary_type}" and parent_code=="{ward_code}"')) > 0:
                            df_queried = df.query(
                                f'name.str.lower()=="{health_facility.name.lower()}" and boundary_type=="{health_facility.boundary_type}" and parent_code=="{ward_code}"')
                            print(df_queried)
                            health_facility_code = df_queried['code'].code
                        else:
                            df = utils.common.append_to_df(df, health_facility)
                        if values[5] is not None and values[6] is not None and health_facility is not None:
                            # Add settlement
                            settlement = get_settlement(values, health_facility_code)
                            if len(df.query(
                                    f'name.str.lower()=="{settlement.name.lower()}" and boundary_type=="{settlement.boundary_type}" and parent_code=="{health_facility_code}"')) > 0:
                                df_queried = df.query(
                                    f'name.str.lower()=="{settlement.name.lower()}" and boundary_type=="{settlement.boundary_type}" and parent_code=="{health_facility_code}"')
                                print(df_queried)
                            else:
                                df = utils.common.append_to_df(df, settlement)
                            if values[22] is not None:
                                for local_monitor in values[22].split(","):
                                    local_monitors_at_lga.add(local_monitor)
                elif values[2] is not None and ward is not None:
                    # Add only health_facility
                    health_facility = get_health_facility(values, ward_code)
                    health_facility_code = health_facility.code
                    if len(df.query(
                            f'name.str.lower()=="{health_facility.name.lower()}" and boundary_type=="{health_facility.boundary_type}" and parent_code=="{ward_code}"')) > 0:
                        df_queried = df.query(
                            f'name.str.lower()=="{health_facility.name.lower()}" and boundary_type=="{health_facility.boundary_type}" and parent_code=="{ward_code}"')
                        print(df_queried)
                    else:
                        df = utils.common.append_to_df(df, health_facility)
                elif values[5] is not None and values[6] is not None and health_facility is not None:
                    # Add settlement
                    settlement = get_settlement(values, health_facility_code)
                    if len(df.query(
                            f'name.str.lower()=="{settlement.name.lower()}" and boundary_type=="{settlement.boundary_type}" and parent_code=="{health_facility_code}"')) > 0:
                        df_queried = df.query(
                            f'name.str.lower()=="{settlement.name.lower()}" and boundary_type=="{settlement.boundary_type}" and parent_code=="{health_facility_code}"')
                        print(df_queried)
                    else:
                        df = utils.common.append_to_df(df, settlement)
                    if values[22] is not None:
                        for local_monitor in values[22].split(","):
                            local_monitors_at_lga.add(local_monitor)
                else:
                    # Empty
                    print(f"Empty Row: {current_row}")
            except ValueError as e:
                print(f"Value Error occurred in row with values:{values}")
                traceback.print_exc()
            except Exception as e:
                print(f"Error occurred in row with values:{values}")
                traceback.print_exc()
        users_df = create_users(local_monitors_at_lga, lga_boundary, user_per_monitor, users_df)
    return df, users_df


def get_ward(values: [], parent_code: str) -> Boundary:
    return Boundary(name=values[1], parent_code=parent_code, boundary_type=BoundaryTypeEnum.WARD.value,
                    code=utils.common.generate_short_code(),
                    boundary_level=constants.BoundaryLevelEnum.WARD.value)


def get_health_facility(values: [], parent_code: str) -> Boundary:
    return Boundary(name=values[2], parent_code=parent_code, boundary_type=BoundaryTypeEnum.HEALTH_FACILITY.value,
                    code=utils.common.generate_short_code(), boundary_level=constants.BoundaryLevelEnum.HEALTH_FACILITY.value)


def get_settlement(values: [], parent_code: str) -> Boundary:
    return Boundary(name=values[6], parent_code=parent_code, boundary_type=BoundaryTypeEnum.SETTLEMENT.value,
                    total_households=math.ceil(values[8] if values[8] is not None else 0),
                    total_individuals=math.ceil(values[7] if values[7] is not None else 0),
                    estimated_bednets=math.ceil(values[12] if values[12] is not None else 0),
                    code=utils.common.generate_short_code(), boundary_level=constants.BoundaryLevelEnum.SETTLEMENT.value,
                    targeted_households=math.ceil(values[8] if values[8] is not None else 0),
                    targeted_individuals=math.ceil(values[7] if values[7] is not None else 0))


def create_users(local_monitors_at_lga: set, lga_boundary: Boundary, user_per_monitor: int,
                 df: DataFrame) -> DataFrame:
    try:
        for local_monitor in local_monitors_at_lga:
            if local_monitor is not None:
                local_monitor = local_monitor.replace("M", "moni")
                lm_name = f"{lga_boundary.name}-{local_monitor}"
                local_monitor_user = User(username=f"{lga_boundary.name}-{local_monitor}".lower(),
                                          password=constants.DEFAULT_USER_PASSWORD,
                                          roles=constants.RoleEnum.LOCAL_MONITOR.value,
                                          employment_type=constants.EmploymentTypeEnum.TEMPORARY.value,
                                          administrative_area=lga_boundary.name,
                                          boundary_code=lga_boundary.code)
                df = utils.common.append_to_user_df(df, local_monitor_user)
                i = 0
                while i < user_per_monitor:
                    i += 1
                    reg_username = f"{lm_name}-{constants.DEFAULT_REG_USER_PREFIX}{i}".lower()
                    reg_user = User(username=f"{reg_username}",
                                    password=constants.DEFAULT_USER_PASSWORD,
                                    roles=constants.RoleEnum.DISTRIBUTOR.value,
                                    employment_type=constants.EmploymentTypeEnum.TEMPORARY.value,
                                    administrative_area=lga_boundary.name,
                                    boundary_code=lga_boundary.code)
                    df = utils.common.append_to_user_df(df, reg_user)
        return df
    except ValueError as e:
        traceback.print_exc()
    except Exception as e:
        traceback.print_exc()


def find_text_in_excel(cell: str, sheet: Sheet) -> str:
    return get_cell(sheet, cell).value


def get_cell(sheet: Sheet, cell: str) -> Range:
    return sheet.range(cell)


def return_visible_sheets(wb: Book) -> [Sheet]:
    visible_sheets = []
    for sheet in wb.sheets:
        if sheet.visible:
            visible_sheets.append(sheet)
    return visible_sheets
