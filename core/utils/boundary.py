import logging

from sqlalchemy import func
from sqlalchemy.orm import Session

import constants
import utils.common
from models.db import Boundary


def upsert_boundary(boundary_name, boundary_enum, previous_boundary, session,
                    boundary_type, filename=None, targets=None):
    if not isinstance(boundary_name, str):
        boundary_name = str(boundary_name)

    boundary = (
        session.query(Boundary)
        .filter(func.lower(Boundary.name) == boundary_name.strip().lower())
        .filter(func.lower(Boundary.parent_code) == str(previous_boundary).strip().lower())
        .filter_by(boundary_type=boundary_type)
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

        # only set targets at the deepest level
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
        logging.error(f"Failed to add {boundary_enum}: {boundary}", exc_info=True)

    return boundary


def upsert_boundary_2(state_name, state_code, boundary_name, session, filename=None):
    state_name = utils.cleanup(state_name)
    boundary_type = constants.BOUNDARIES[boundary_name]["name"]

    state_boundary = (
        session.query(Boundary)
        .filter(func.lower(Boundary.name) == state_name.strip().lower())
        .filter(func.lower(Boundary.code) == str(state_code).strip().lower())
        .filter_by(boundary_type=boundary_type)
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


def query_boundary_without_parent(boundary_name, session, boundary_type):
    return session.query(Boundary).filter(
        func.lower(Boundary.name) == boundary_name.strip().lower()).filter_by(
        boundary_type=boundary_type).first()
