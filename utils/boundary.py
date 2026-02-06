import logging

from sqlalchemy import func
from sqlalchemy.orm import Session

import constants
import utils.common
from constants import BOUNDARY_2_CODE
from models.db import Boundary


def upsert_boundary_2(state_name: str, state_code: str, boundary_name: str, session: Session, filename: str = None) -> Boundary:
    state_name = utils.cleanup(state_name)
    state_boundary = session.query(Boundary).filter(func.lower(Boundary.name) == state_name.strip().lower()).filter_by(
        code=state_code, boundary_type=constants.BOUNDARIES[boundary_name]["name"]).first()
    if state_boundary is None:
        try:
            state_boundary = Boundary(name=state_name.strip(), name_in_english=state_name.strip(),
                                      boundary_type=constants.BOUNDARIES[boundary_name]["name"], code=state_code,
                                      boundary_level=constants.BOUNDARIES[boundary_name]["level"],
                                      parent_code=constants.MOZ_COUNTRY_CODE, filename=filename,
                                      )
            session.add(state_boundary)
            session.commit()
        except Exception as e:
            logging.error(
                f"Exception occurred while adding state boundary: {e}", exc_info=True)

    return state_boundary


def upsert_boundary_3(lga_name: str, state_boundary: Boundary, boundary_name: str, session: Session, target: str,
                      filename: str = None) -> Boundary:
    lga_name = utils.cleanup(lga_name)
    boundary_type = constants.BOUNDARIES[boundary_name]["name"]
    lga_boundary = query_boundary(lga_name, session, boundary_type)
    if lga_boundary is None:
        try:
            lga_boundary = Boundary(name=lga_name.strip(), name_in_english=lga_name.strip(),
                                    code=utils.common.generate_short_code(),
                                    boundary_type=boundary_type,
                                    boundary_level=constants.BOUNDARIES[boundary_name]["level"],
                                    parent_code=state_boundary.code, filename=filename,
                                    checklist_target=target)
            session.add(lga_boundary)
            session.commit()
        except Exception as e:
            logging.error(
                f"Exception occurred while adding lga boundary: {e}", exc_info=True)

    return lga_boundary


def upsert_ward(ward_name: str, lga_boundary: Boundary, session: Session,
                filename: str = None) -> Boundary:

    #                                filename: str = None) -> Boundary:
    # >>>>>>> origin/master
    ward_boundary = session.query(Boundary).filter(func.lower(Boundary.name) == ward_name.strip().lower()).filter_by(
        parent_code=lga_boundary.code, boundary_type=constants.BoundaryTypeEnum.WARD.value).first()
    if ward_boundary is None:
        try:
            ward_boundary = Boundary(name=ward_name.strip(), name_in_english=ward_name.strip(),
                                     code=utils.common.generate_short_code(),
                                     boundary_type=constants.BoundaryTypeEnum.WARD.value,
                                     boundary_level=constants.BoundaryLevelEnum.WARD.value,
                                     parent_code=lga_boundary.code, filename=filename)
            session.add(ward_boundary)
            session.commit()
        except Exception as e:
            logging.error(
                f"Exception occurred while adding ward boundary: {e}", exc_info=True)

    return ward_boundary


def upsert_health_facility(health_facility_name: str, ward_boundary: Boundary, session: Session, filename: str = None) -> Boundary:
    health_facility_name = utils.cleanup(health_facility_name)
    health_facility_boundary = session.query(Boundary).filter(func.lower(Boundary.name) == health_facility_name.strip().lower()).filter_by(
        parent_code=ward_boundary.code, boundary_type=constants.BoundaryTypeEnum.HEALTH_FACILITY.value).first()
    if health_facility_boundary is None:
        try:
            health_facility_boundary = Boundary(name=health_facility_name.strip(), name_in_english=health_facility_name.strip(),
                                                code=utils.common.generate_short_code(),
                                                boundary_type=constants.BoundaryTypeEnum.HEALTH_FACILITY.value,
                                                boundary_level=constants.BoundaryLevelEnum.HEALTH_FACILITY.value,
                                                parent_code=ward_boundary.code, filename=filename)
            session.add(health_facility_boundary)
            session.commit()
        except Exception as e:
            logging.error(
                f"Exception occurred while adding health_facility boundary: {e}", exc_info=True)

    return health_facility_boundary


def upsert_settlement(settlement_name: str, health_facility_boundary: Boundary, session: Session, filename: str = None,

                      targets:  dict = None):

    settlement_boundary = session.query(Boundary).filter(func.lower(Boundary.name) == settlement_name.strip().lower()).filter_by(
        parent_code=health_facility_boundary.code, boundary_type=constants.BoundaryTypeEnum.SETTLEMENT.value).first()
    try:
        if settlement_boundary is None:
            settlement_boundary = Boundary(name=settlement_name.strip(), name_in_english=settlement_name.strip(),
                                           code=utils.common.generate_short_code(),
                                           boundary_type=constants.BoundaryTypeEnum.SETTLEMENT.value,
                                           boundary_level=constants.BoundaryLevelEnum.SETTLEMENT.value,
                                           parent_code=health_facility_boundary.code, filename=filename)
# =======
#                                         code=utils.common.generate_short_code(),
#                                         boundary_type=constants.BoundaryTypeEnum.SETTLEMENT.value,
#                                         boundary_level=constants.BoundaryLevelEnum.SETTLEMENT.value,
#                                         parent_code=health_facility_boundary.code, filename=filename)
# >>>>>>> origin/master
        else:
            if filename is not None:
                settlement_boundary.filename = filename
        if targets:
            for target_name, target_value in targets.items():
                setattr(settlement_boundary, target_name, target_value)
        session.add(settlement_boundary)
        session.commit()
    except Exception as e:
        logging.error(
            f"Exception occurred while adding Village boundary: {settlement_boundary}", exc_info=True)

    return settlement_boundary


def update_settlement(health_facility_code: str, session: Session, count_3_11: int = 0, count_12_59: int = 0,
                      count_spaq_1: int = 0, count_spaq_2: int = 0):

    settlement_boundary = session.query(Boundary).filter(func.lower(Boundary.health_facility) == hf_name.lower()).filter_by(
        parent_code=health_facility_code, boundary_type=constants.BoundaryTypeEnum.SETTLEMENT.value).first()
    try:
        if settlement_boundary is not None:
            settlement_boundary.total_3_11 = count_3_11
            settlement_boundary.targeted_3_11 = count_3_11
            settlement_boundary.total_12_59 = count_12_59
            settlement_boundary.targeted_12_59 = count_12_59
            settlement_boundary.total_individuals = count_3_11 + count_12_59
            settlement_boundary.total_spaq_1 = count_spaq_1
            settlement_boundary.targeted_spaq_1 = count_spaq_1
            settlement_boundary.total_spaq_2 = count_spaq_2
            settlement_boundary.targeted_spaq_2 = count_spaq_2
            settlement_boundary.total_spaq = count_spaq_1 + count_spaq_2
            session.add(settlement_boundary)
            session.commit()
        else:
            logging.warning(
                "Couldn't find any settlement linked with this Health facility")
    except Exception as e:
        logging.error(
            f"Exception occurred while updating Village boundary: {settlement_boundary}", exc_info=True)


def query_boundary(dist_name: str, session: Session, boundary_type: str) -> Boundary | None:
    # =======
    #             logging.warning("Couldn't find any settlement linked with this Health facility")
    #     except Exception as e:
    #         logging.error(f"Exception occurred while updating Village boundary: {settlement_boundary}", exc_info=True)

    # def query_boundary(dist_name: str, session: Session, boundary_type : str) -> Boundary | None:
    # >>>>>>> origin/master
    return session.query(Boundary).filter(
        func.lower(Boundary.name) == dist_name.strip().lower()).filter_by(
        parent_code=BOUNDARY_2_CODE, boundary_type=boundary_type).first()


def query_boundary_without_parent(dist_name: str, session: Session, boundary_type: str) -> Boundary | None:
    return session.query(Boundary).filter(
        func.lower(Boundary.name) == dist_name.strip().lower()).filter_by(
        boundary_type=boundary_type).first()


def upsert_boundary(
    boundary_name: str,
    boundary_enum: str,
    previous_boundary: str,
    session: Session,
    boundary_type: str,
    filename: str = None,
    targets: dict = None
):
    if not isinstance(boundary_name, str):
        boundary_name = str(boundary_name)

    boundary = (
        session.query(Boundary)
        .filter(func.lower(Boundary.name) == boundary_name.strip().lower())
        .filter_by(parent_code=previous_boundary, boundary_type=boundary_type)
        .first()
    )

    try:
        if boundary is None:
            boundary = Boundary(
                name=boundary_name.strip(),
                name_in_english=boundary_name.strip(),
                code=utils.common.generate_short_code(),
                boundary_type=boundary_type,
                boundary_level=constants.BOUNDARIES[boundary_enum]["level"],
                parent_code=previous_boundary,
                filename=filename,
                campaign_start_date=getattr(constants, 'CAMPAIGN_START_DATE', None),
                campaign_end_date=getattr(constants, 'CAMPAIGN_END_DATE', None),
            )
        elif filename is not None:
            boundary.filename = filename

        # ─────────────────────────────────────────────────────────────
        # Touch targets ONLY for the deepest boundary level
        # ─────────────────────────────────────────────────────────────
        max_level = max(info["level"] for info in constants.BOUNDARIES.values())
        if boundary.boundary_level == max_level:
            for i in range(1, 4):
                setattr(boundary, f"target_{i}", 0)
                setattr(boundary, f"total_{i}",  0)

            if targets:
                for t_name, t_value in targets.items():
                    idx = int(t_name.split("_")[1])
                    setattr(boundary, f"target_{idx}", t_value)
                    setattr(boundary, f"total_{idx}",  t_value)

        session.add(boundary)
        session.commit()
    except Exception:
        logging.error(
            f"Exception occurred while adding {boundary_enum} boundary: {boundary}",
            exc_info=True
        )

    return boundary


def upsert_boundary_2(
    state_name: str,
    state_code: str,
    boundary_name: str,
    session: Session,
    filename: str = None
) -> Boundary:
    state_name = utils.cleanup(state_name)
    boundary_type = constants.BOUNDARIES[boundary_name]["name"]

    state_boundary = (
        session.query(Boundary)
        .filter(func.lower(Boundary.name) == state_name.strip().lower())
        .filter_by(code=state_code, boundary_type=boundary_type)
        .first()
    )

    if state_boundary is None:
        state_boundary = Boundary(
            name=state_name.strip(),
            name_in_english=state_name.strip(),
            code=state_code,
            boundary_type=boundary_type,
            boundary_level=constants.BOUNDARIES[boundary_name]["level"],
            parent_code=constants.BOUNDARY_1_CODE,
            campaign_start_date=getattr(constants, 'CAMPAIGN_START_DATE', None),
            campaign_end_date=getattr(constants, 'CAMPAIGN_END_DATE', None),
        )
        session.add(state_boundary)
        session.commit()

    if filename:
        state_boundary.filename = filename
        session.add(state_boundary)
        session.commit()

    return state_boundary
