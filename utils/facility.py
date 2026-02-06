import logging

from sqlalchemy import func
from sqlalchemy.orm import Session
from typing import List


from constants import BOUNDARY_2_CODE, constants

from models.db import Boundary, Facility

boundary_map = {}


def create_health_facility(
    facility_name: str,
    mapping_boundary: str,    # still used for administrative_area
    session: Session,
    facility_type: str,
    filename: str | None = None,
    target: int = 0
):
    """Create or update a Facility record that corresponds to a Boundary.

    Args:
        facility_name: The displayed name of the facility.
        mapping_boundary: The administrative area name supplied in the CSV.
        session: SQLAlchemy session.
        facility_type: One of the accepted facility-type strings.
        filename: Optional source-file name for traceability.
        target: Integer target value (0 if not provided).
    """

    # 0) Exact lookup string (no "LGA Store" suffix removal)
    lookup_name = facility_name.strip()

    # 1) Build the base query: match by name, ignore internal spaces (case‑insensitive)
    q = session.query(Boundary).filter(
        func.replace(func.lower(Boundary.name), " ", "") ==
        lookup_name.lower().replace(" ", "")
    ).filter(Boundary.boundary_level == 4)

    if facility_type == "LGA Facility":                  # 1a) optional LGA filter
        lga_type = constants.BOUNDARIES["BOUNDARY_4"]["name"]
        q = q.filter_by(boundary_type=lga_type)

    matches = q.all()                                     # 2) fetch
    if not matches:
        logging.warning(f"{facility_name!r} not found in Boundary table")
        return

    for boundary in matches:
        boundary_code = boundary.code   # 3) code
        # 4) ancestor chain
        ancestors = []
        code = boundary.parent_code
        while code and code != constants.BOUNDARY_1_CODE:
            ancestors.append(code)
            parent = session.query(Boundary).filter_by(code=code).first()
            code = parent.parent_code if parent else None
        parent_chain = ",".join(ancestors)

        # parent boundary's name so we can match on it
        parent_name = None
        if boundary.parent_code:
            parent = session.query(Boundary).filter_by(
                code=boundary.parent_code).first()
            parent_name = parent.name if parent else None

        # 5) Upsert for this boundary – exact name match, ignoring spaces
        existing = (
            session.query(Facility)
            .join(Boundary, Facility.boundary_code == Boundary.code)
            .filter(
                func.replace(func.lower(Facility.facility_name), " ", "") ==
                lookup_name.lower().replace(" ", "")
            )
            .filter(Facility.boundary_code == boundary_code)
            .filter(Facility.administrative_area == parent_name)
            .filter(Boundary.boundary_level == 4)
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
                logging.info(
                    f"Inserted '{facility_name}' → boundary_code={boundary_code}")
            except Exception:
                session.rollback()
                logging.error(
                    f"Error inserting '{facility_name}' for boundary_code={boundary_code}",
                    exc_info=True,
                )
        else:
            existing.target = target
            existing.facility_type = facility_type
            existing.parent_code = parent_chain
            existing.filename = filename
            session.add(existing)
            try:
                session.commit()
                logging.info(
                    f"Updated '{facility_name}', boundary_code={boundary_code}")
            except Exception:
                session.rollback()
                logging.error(
                    f"Error updating '{facility_name}' for boundary_code={boundary_code}",
                    exc_info=True,
                )


def update_health_facility(facility_name: str, district_boundary: Boundary, session: Session, target: int = 0, hf_code: str = None) -> Facility | None:
    if district_boundary is not None:
        facility = (session.query(Facility).filter(func.lower(Facility.facility_name) == facility_name.lower())
                    .filter_by(parent_code=(district_boundary.code)).first())
        if facility is not None:
            if target != 0:
                facility.target = target
            if hf_code is not None:
                facility.hf_code = hf_code
            session.add(facility)
            session.commit()
        return facility
    return None


def create_com_supervisor_facility(facility_name: str, district_boundary: Boundary, ward_boundary_code: str,
                                   locality_boundary_name: str, session: Session, filename: str = None,
                                   target: int = 0):
    facility = (session.query(Facility).filter(func.lower(Facility.facility_name) == facility_name.lower())
                .filter_by(boundary_code=ward_boundary_code).first())
    if facility is None:
        try:
            facility = Facility(facility_name=facility_name,
                                boundary_code=ward_boundary_code,
                                facility_type=FacilityTypeEnum.COMMUNITY_SUPERVISOR.value,
                                administrative_area=locality_boundary_name,
                                is_permanent='FALSE', filename=filename, target=target,
                                parent_code=(district_boundary.code))
            session.add(facility)
            session.commit()
            logging.info(f"Creating new facility with Facility Name: {facility.facility_name},"
                         f" boundary_code={facility.boundary_code}")
        except Exception as e:
            logging.error(f"Unable to insert facility with Facility Name: {facility.facility_name},"
                          f" boundary_code={facility.boundary_code}", exc_info=True)
    else:
        logging.warning(
            f"Reusing CS with name: {facility.facility_name}, boundary_code:{ward_boundary_code}")


def cleanup_facility_name(name: str) -> str:
    return name.replace(" ", "")
