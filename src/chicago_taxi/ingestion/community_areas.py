"""Validate the complete community-area source snapshot."""
def normalize_areas(rows):
    if not rows:raise ValueError("Empty community area snapshot")
    result=[];seen=set()
    for row in rows:
        key=str(int(row["area_num_1"]))
        geometry=row.get("the_geom",{})
        if key in seen or not 1<=int(key)<=77:raise ValueError("Duplicate/invalid community ID")
        if not row.get("community") or geometry.get("type") not in ("Polygon","MultiPolygon") or not geometry.get("coordinates"):
            raise ValueError("Missing community name or polygon")
        seen.add(key);result.append((key,row))
    return result

