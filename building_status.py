import psycopg2

def sort_fn(x):
    ward_year = 0
    if not x["wardId"]:
        return ward_year
    if "wardYear" in x["wardId"]:
        ward_year = x["wardId"]["wardYear"]
    return ward_year

def get_building_details(building_id):  
    cur.execute("SELECT buildingid, lbid, registry FROM ldms_building WHERE buildingid = %s", (building_id,))
    result = cur.fetchone()
    building_id, lbid, registry = result
    #print(registry)
    ward_year = None
    ward_id = None
    sorted_buildingdoors = sorted(registry["buildingDoors"],key=sort_fn,reverse=True)
    if sorted_buildingdoors[0]["wardId"] and "wardYear" in sorted_buildingdoors[0]["wardId"]:
        ward_year = sorted_buildingdoors[0]["wardId"]["wardYear"]
        ward_id = sorted_buildingdoors[0]["wardId"]["wardId"]
    door_no = sorted_buildingdoors[0]["doorNumber"]
    status = registry["status"]
    building_status = registry["buildingStatus"]
    owners = registry["owners"]
    owner_names = ""
    for owner in owners:  
        if owner_names: 
            owner_names = f"{owner_names},{owner["addressId"]["name"]}" 
        else:
            owner_names = owner["addressId"]["name"]
    return building_id, lbid, owner_names, status, building_status, ward_id, ward_year, door_no
# Example usage:
conn = psycopg2.connect(
        database="LDMS", 
        user="postgres", 
        password="Sarah1903@", 
        host="localhost", 
        port="5432"
        )
cur = conn.cursor()
cur.execute("SELECT buildingid FROM ldms_building")   
#result = get_building_details(building_id=30179010000001) 
building_ids = cur.fetchall()
try:
    for building_id in building_ids:
        print(building_id)
        result = get_building_details(building_id=building_id[0])
        building_id, lbid, owner_names, status, building_status, ward_id, ward_year, door_no = result
        cur.execute("INSERT INTO ldms_building_status (buildingid, lbid, ward_year, ward_id, owner_name, status, building_status, door_no) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (building_id, lbid, ward_year, ward_id, owner_names, status, building_status, door_no))
    conn.commit()
except Exception as e:
    conn.rollback()
    print(f"Error occured: {e}. Changes rolled back")

cur.close()
conn.close()