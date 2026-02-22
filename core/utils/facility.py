import logging

from sqlalchemy import func
from sqlalchemy.orm import Session

from constants import constants
from models.db import Boundary, Facility


def create_health_facility(facility_name, mapping_boundary, session, facility_type,
                           filename=None, target=0):
    lookup_name = facility_name.strip()

    facility_level = getattr(constants, 'FACILITY_BOUNDARY_LEVEL',
                             max(info["level"] for info in constants.BOUNDARIES.values()))

    # case-insensitive match, ignore spaces
    q = session.query(Boundary).filter(
        func.replace(func.lower(Boundary.name), " ", "") ==
        lookup_name.lower().replace(" ", "")
    ).filter(Boundary.boundary_level == facility_level)

    if facility_type == "LGA Facility":
        lga_type = constants.BOUNDARIES["BOUNDARY_4"]["name"]
        q = q.filter_by(boundary_type=lga_type)

    matches = q.all()
    if not matches:
        logging.warning(f"{facility_name!r} not found in Boundary table")
        return

    for boundary in matches:
        boundary_code = boundary.code

        # walk up to build parent chain
        ancestors = []
        code = boundary.parent_code
        while code and code != constants.BOUNDARY_1_CODE:
            ancestors.append(code)
            parent = session.query(Boundary).filter_by(code=code).first()
            code = parent.parent_code if parent else None
        parent_chain = ",".join(ancestors)

        parent_name = None
        if boundary.parent_code:
            parent = session.query(Boundary).filter_by(
                code=boundary.parent_code).first()
            parent_name = parent.name if parent else None

        existing = (
            session.query(Facility)
            .join(Boundary, Facility.boundary_code == Boundary.code)
            .filter(
                func.replace(func.lower(Facility.facility_name), " ", "") ==
                lookup_name.lower().replace(" ", "")
            )
            .filter(Facility.boundary_code == boundary_code)
            .filter(Facility.administrative_area == parent_name)
            .filter(Boundary.boundary_level == facility_level)
            .first()
        )

        if existing is None:
            facility = Facility(
                facility_name=facility_name.strip(),
                is_permanent="TRUE",
                facility_type=facility_type,
                boundary_code=boundary_code,
                administrative_area=mapping_boundary,
                storage=0,
                parent_code=parent_chain,
                target=target,
                filename=filename
            )
            session.add(facility)
            try:
                session.commit()
            except Exception:
                session.rollback()
                logging.error(f"Error inserting '{facility_name}'", exc_info=True)
        else:
            existing.target = target
            existing.facility_type = facility_type
            existing.parent_code = parent_chain
            existing.filename = filename
            session.add(existing)
            try:
                session.commit()
            except Exception:
                session.rollback()
                logging.error(f"Error updating '{facility_name}'", exc_info=True)


def update_health_facility(facility_name, district_boundary, session, target=0, hf_code=None):
    if district_boundary is not None:
        facility = (session.query(Facility)
                    .filter(func.lower(Facility.facility_name) == facility_name.lower())
                    .filter_by(parent_code=district_boundary.code).first())
        if facility is not None:
            if target != 0:
                facility.target = target
            if hf_code is not None:
                facility.hf_code = hf_code
            session.add(facility)
            session.commit()
        return facility
    return None
